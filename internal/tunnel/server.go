package tunnel

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/Kavun-Sama/salute-jazz-rtc-tunnel/internal/crypto"
	"github.com/Kavun-Sama/salute-jazz-rtc-tunnel/internal/mux"
	"github.com/Kavun-Sama/salute-jazz-rtc-tunnel/internal/transport"
)

// ConnectRequest is the first frame sent by the client on a new stream.
type ConnectRequest struct {
	Cmd  string `json:"cmd"`
	Addr string `json:"addr"`
	Port int    `json:"port"`
}

// Server accepts mux streams and proxies them to TCP connections.
type Server struct {
	peers  []transport.Transport
	cipher *crypto.Cipher
	mx     *mux.Mux
	dns    string
	socks  string // upstream SOCKS5 proxy

	conns   map[mux.StreamKey]net.Conn
	connsMu sync.RWMutex

	log *slog.Logger
}

// ServerConfig holds configuration for creating a tunnel server.
type ServerConfig struct {
	Peers  []transport.Transport
	Cipher *crypto.Cipher
	DNS    string
	Socks  string
	Logger *slog.Logger
}

// NewServer creates a new tunnel server.
func NewServer(cfg ServerConfig) *Server {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	s := &Server{
		peers:  cfg.Peers,
		cipher: cfg.Cipher,
		dns:    cfg.DNS,
		socks:  cfg.Socks,
		conns:  make(map[mux.StreamKey]net.Conn),
		log:    cfg.Logger.With(slog.String("component", "tunnel/server")),
	}

	s.mx = mux.NewMux(s.sendFrame, s.onNewStream, cfg.Logger)

	return s
}

// Run starts the tunnel server and blocks until context is cancelled.
func (s *Server) Run(ctx context.Context) error {
	// Register data handlers on all peers
	for _, p := range s.peers {
		p.SetOnData(func(data []byte) {
			plaintext, err := s.cipher.Decrypt(data)
			if err != nil {
				s.log.Warn("decrypt failed", "error", err)
				return
			}
			s.mx.HandleFrame(plaintext)
		})

		p.SetOnReconnect(func() {
			s.log.Info("peer reconnected, resetting mux state")
			s.closeAllConns()
			s.mx.CloseAll()
		})
	}

	s.log.Info("tunnel server running")

	<-ctx.Done()

	s.closeAllConns()
	s.mx.CloseAll()

	return nil
}

func (s *Server) onNewStream(stream *mux.Stream) {
	go s.handleStream(stream)
}

func (s *Server) handleStream(stream *mux.Stream) {
	s.log.Debug("new stream", "clientID", stream.Key.ClientID, "sid", stream.Key.SID)

	// First frame should be a ConnectRequest
	data := stream.Read()
	if data == nil {
		s.log.Debug("stream closed before connect request")
		return
	}

	var req ConnectRequest
	if err := json.Unmarshal(data, &req); err != nil {
		s.log.Warn("invalid connect request", "error", err, "data", string(data))
		stream.Close()
		return
	}

	if req.Cmd != cmdConnect {
		s.log.Warn("unknown command", "cmd", req.Cmd)
		stream.Close()
		return
	}

	addr := fmt.Sprintf("%s:%d", req.Addr, req.Port)
	s.log.Info("connecting", "addr", addr, "clientID", stream.Key.ClientID, "sid", stream.Key.SID)

	// Dial the target
	var conn net.Conn
	var err error

	if s.socks != "" {
		conn, err = s.dialViaSocks(addr)
	} else {
		dialer := &net.Dialer{
			Timeout: 10 * time.Second,
		}
		if s.dns != "" {
			dialer.Resolver = &net.Resolver{
				PreferGo: true,
				Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
					d := net.Dialer{Timeout: 5 * time.Second}
					return d.DialContext(ctx, "udp", s.dns)
				},
			}
		}
		conn, err = dialer.Dial("tcp", addr)
	}

	if err != nil {
		s.log.Warn("dial failed", "addr", addr, "error", err)
		stream.Close()
		return
	}

	s.connsMu.Lock()
	s.conns[stream.Key] = conn
	s.connsMu.Unlock()

	var once sync.Once
	cleanup := func() {
		conn.Close()
		s.connsMu.Lock()
		delete(s.conns, stream.Key)
		s.connsMu.Unlock()
		stream.Close()
	}
	defer once.Do(cleanup)

	// Send confirmation byte (0x00)
	if err := stream.Write([]byte{0x00}); err != nil {
		s.log.Warn("send confirmation failed", "error", err)
		return
	}

	// Bidirectional proxy
	done := make(chan struct{}, 2)

	// TCP -> Stream
	go func() {
		defer func() { done <- struct{}{} }()
		buf := make([]byte, 32*1024)
		for {
			n, err := conn.Read(buf)
			if n > 0 {
				if writeErr := s.writeToStream(stream, buf[:n]); writeErr != nil {
					s.log.Debug("write to stream failed", "error", writeErr)
					return
				}
			}
			if err != nil {
				return
			}
		}
	}()

	// Stream -> TCP
	go func() {
		defer func() { done <- struct{}{} }()
		for {
			data := stream.Read()
			if data == nil {
				return
			}
			if _, err := conn.Write(data); err != nil {
				return
			}
		}
	}()

	// Wait for either direction to finish, then force the other side to unwind too.
	<-done
	once.Do(cleanup)
	<-done
}

// writeToStream writes data to a stream, respecting backpressure.
func (s *Server) writeToStream(stream *mux.Stream, data []byte) error {
	return stream.Write(data)
}

// sendFrame encrypts and sends a frame through a transport peer.
func (s *Server) sendFrame(data []byte) error {
	if len(s.peers) == 0 {
		return fmt.Errorf("no transport peers configured")
	}

	clientID, sid, _, flags, _, err := mux.ParseFrame(data)
	if err != nil {
		return fmt.Errorf("parse mux frame: %w", err)
	}

	if flags&mux.FlagReset != 0 {
		for _, peer := range s.peers {
			if err := sendEncryptedFrame(peer, s.cipher, data); err != nil {
				return err
			}
		}
		return nil
	}

	return sendEncryptedFrame(s.peers[streamPeerIndex(len(s.peers), clientID, sid)], s.cipher, data)
}

func (s *Server) closeAllConns() {
	s.connsMu.Lock()
	conns := s.conns
	s.conns = make(map[mux.StreamKey]net.Conn)
	s.connsMu.Unlock()

	for _, c := range conns {
		c.Close()
	}
}

// dialViaSocks connects to addr through an upstream SOCKS5 proxy.
func (s *Server) dialViaSocks(addr string) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", s.socks, 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("dial socks proxy: %w", err)
	}

	// SOCKS5 handshake: no auth
	if _, err := conn.Write([]byte{0x05, 0x01, 0x00}); err != nil {
		conn.Close()
		return nil, fmt.Errorf("socks auth: %w", err)
	}

	if err := readSocksAuthResponse(conn); err != nil {
		conn.Close()
		return nil, err
	}

	// Parse host:port
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("split host port: %w", err)
	}

	portNum, err := strconv.Atoi(port)
	if err != nil || portNum < 0 || portNum > 65535 {
		conn.Close()
		return nil, fmt.Errorf("invalid port %q", port)
	}

	// CONNECT request with domain
	req := []byte{0x05, 0x01, 0x00, 0x03, byte(len(host))}
	req = append(req, []byte(host)...)
	req = append(req, byte(portNum>>8), byte(portNum&0xFF))

	if _, err := conn.Write(req); err != nil {
		conn.Close()
		return nil, fmt.Errorf("socks connect: %w", err)
	}

	if err := readSocksConnectResponse(conn); err != nil {
		conn.Close()
		return nil, err
	}

	return conn, nil
}

func readSocksAuthResponse(r io.Reader) error {
	resp := make([]byte, 2)
	if _, err := io.ReadFull(r, resp); err != nil {
		return fmt.Errorf("socks auth response: %w", err)
	}
	if resp[0] != 0x05 || resp[1] != 0x00 {
		return fmt.Errorf("socks auth rejected")
	}
	return nil
}

func readSocksConnectResponse(r io.Reader) error {
	header := make([]byte, 4)
	if _, err := io.ReadFull(r, header); err != nil {
		return fmt.Errorf("socks connect response header: %w", err)
	}
	if header[0] != 0x05 {
		return fmt.Errorf("invalid socks version: %d", header[0])
	}
	if header[1] != 0x00 {
		return fmt.Errorf("socks connect failed: rep=%d", header[1])
	}

	addrLen := 0
	switch header[3] {
	case 0x01:
		addrLen = net.IPv4len
	case 0x03:
		domainLen := make([]byte, 1)
		if _, err := io.ReadFull(r, domainLen); err != nil {
			return fmt.Errorf("socks connect domain length: %w", err)
		}
		addrLen = int(domainLen[0])
	case 0x04:
		addrLen = net.IPv6len
	default:
		return fmt.Errorf("unsupported socks bind atyp: %d", header[3])
	}

	if addrLen > 0 {
		addr := make([]byte, addrLen)
		if _, err := io.ReadFull(r, addr); err != nil {
			return fmt.Errorf("socks connect bind addr: %w", err)
		}
	}

	port := make([]byte, 2)
	if _, err := io.ReadFull(r, port); err != nil {
		return fmt.Errorf("socks connect bind port: %w", err)
	}

	return nil
}

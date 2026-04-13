package socks

import (
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
)

const (
	socks5Version = 0x05
	cmdConnect    = 0x01
	atypIPv4      = 0x01
	atypDomain    = 0x03
	atypIPv6      = 0x04

	repSuccess         = 0x00
	repGeneralFailure  = 0x01
	repHostUnreachable = 0x04
	repCmdNotSupported = 0x07
	repAtypNotSupported = 0x08
)

// OnConnectFunc is called when a SOCKS5 CONNECT request is received.
// addr is "host:port" format.
type OnConnectFunc func(conn net.Conn, host string, port int)

// Server is a minimal SOCKS5 server supporting only CONNECT with no auth.
type Server struct {
	listener  net.Listener
	onConnect OnConnectFunc
	log       *slog.Logger
}

// NewServer creates a new SOCKS5 server.
func NewServer(onConnect OnConnectFunc, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{
		onConnect: onConnect,
		log:       logger.With(slog.String("component", "socks5")),
	}
}

// ListenAndServe starts the SOCKS5 server on the given address.
func (s *Server) ListenAndServe(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	s.listener = ln
	s.log.Info("SOCKS5 listening", "addr", addr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			return fmt.Errorf("accept: %w", err)
		}
		go s.handleConn(conn)
	}
}

// Close stops the SOCKS5 server.
func (s *Server) Close() error {
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}

func (s *Server) handleConn(conn net.Conn) {
	defer func() {
		if r := recover(); r != nil {
			s.log.Error("panic in socks5 handler", "error", r)
		}
	}()

	// Auth negotiation
	if err := s.negotiate(conn); err != nil {
		s.log.Debug("auth negotiation failed", "error", err, "remote", conn.RemoteAddr())
		conn.Close()
		return
	}

	// Read CONNECT request
	host, port, err := s.readRequest(conn)
	if err != nil {
		s.log.Debug("read request failed", "error", err, "remote", conn.RemoteAddr())
		conn.Close()
		return
	}

	s.log.Debug("CONNECT request", "host", host, "port", port, "remote", conn.RemoteAddr())

	// Hand off to callback — the callback is responsible for:
	// 1. Sending the SOCKS5 success/failure reply
	// 2. Proxying data
	// 3. Closing the connection
	s.onConnect(conn, host, port)
}

// negotiate handles the SOCKS5 auth method negotiation.
func (s *Server) negotiate(conn net.Conn) error {
	// Read version + nmethods
	header := make([]byte, 2)
	if _, err := io.ReadFull(conn, header); err != nil {
		return fmt.Errorf("read auth header: %w", err)
	}

	if header[0] != socks5Version {
		return fmt.Errorf("unsupported SOCKS version: %d", header[0])
	}

	nmethods := int(header[1])
	methods := make([]byte, nmethods)
	if _, err := io.ReadFull(conn, methods); err != nil {
		return fmt.Errorf("read auth methods: %w", err)
	}

	// We only support NO AUTH (0x00)
	hasNoAuth := false
	for _, m := range methods {
		if m == 0x00 {
			hasNoAuth = true
			break
		}
	}

	if !hasNoAuth {
		conn.Write([]byte{socks5Version, 0xFF}) // no acceptable methods
		return fmt.Errorf("no acceptable auth method")
	}

	// Reply: no auth required
	_, err := conn.Write([]byte{socks5Version, 0x00})
	return err
}

// readRequest reads and parses a SOCKS5 CONNECT request.
func (s *Server) readRequest(conn net.Conn) (host string, port int, err error) {
	// VER CMD RSV ATYP ...
	header := make([]byte, 4)
	if _, err := io.ReadFull(conn, header); err != nil {
		return "", 0, fmt.Errorf("read request header: %w", err)
	}

	if header[0] != socks5Version {
		return "", 0, fmt.Errorf("unsupported version: %d", header[0])
	}

	if header[1] != cmdConnect {
		s.sendReply(conn, repCmdNotSupported)
		return "", 0, fmt.Errorf("unsupported command: %d", header[1])
	}

	atyp := header[3]

	switch atyp {
	case atypIPv4:
		addr := make([]byte, 4)
		if _, err := io.ReadFull(conn, addr); err != nil {
			return "", 0, fmt.Errorf("read IPv4 addr: %w", err)
		}
		host = net.IP(addr).String()

	case atypDomain:
		lenBuf := make([]byte, 1)
		if _, err := io.ReadFull(conn, lenBuf); err != nil {
			return "", 0, fmt.Errorf("read domain length: %w", err)
		}
		domain := make([]byte, lenBuf[0])
		if _, err := io.ReadFull(conn, domain); err != nil {
			return "", 0, fmt.Errorf("read domain: %w", err)
		}
		host = string(domain)

	case atypIPv6:
		addr := make([]byte, 16)
		if _, err := io.ReadFull(conn, addr); err != nil {
			return "", 0, fmt.Errorf("read IPv6 addr: %w", err)
		}
		host = net.IP(addr).String()

	default:
		s.sendReply(conn, repAtypNotSupported)
		return "", 0, fmt.Errorf("unsupported address type: %d", atyp)
	}

	// Read port (2 bytes, big endian)
	portBuf := make([]byte, 2)
	if _, err := io.ReadFull(conn, portBuf); err != nil {
		return "", 0, fmt.Errorf("read port: %w", err)
	}
	port = int(binary.BigEndian.Uint16(portBuf))

	return host, port, nil
}

// SendSuccess sends a SOCKS5 success reply to the client.
func SendSuccess(conn net.Conn) error {
	// VER REP RSV ATYP ADDR PORT
	reply := []byte{
		socks5Version, repSuccess, 0x00,
		atypIPv4, 0, 0, 0, 0, // bind addr 0.0.0.0
		0, 0, // bind port 0
	}
	_, err := conn.Write(reply)
	return err
}

// SendFailure sends a SOCKS5 failure reply to the client.
func SendFailure(conn net.Conn) error {
	reply := []byte{
		socks5Version, repGeneralFailure, 0x00,
		atypIPv4, 0, 0, 0, 0,
		0, 0,
	}
	_, err := conn.Write(reply)
	return err
}

// SendHostUnreachable sends a SOCKS5 host unreachable reply.
func SendHostUnreachable(conn net.Conn) error {
	reply := []byte{
		socks5Version, repHostUnreachable, 0x00,
		atypIPv4, 0, 0, 0, 0,
		0, 0,
	}
	_, err := conn.Write(reply)
	return err
}

func (s *Server) sendReply(conn net.Conn, rep byte) {
	reply := []byte{
		socks5Version, rep, 0x00,
		atypIPv4, 0, 0, 0, 0,
		0, 0,
	}
	conn.Write(reply)
}

// FormatAddr formats host and port into a "host:port" string.
func FormatAddr(host string, port int) string {
	return net.JoinHostPort(host, strconv.Itoa(port))
}

package tunnel

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Kavun-Sama/salute-jazz-rtc-tunnel/internal/crypto"
	"github.com/Kavun-Sama/salute-jazz-rtc-tunnel/internal/mux"
	"github.com/Kavun-Sama/salute-jazz-rtc-tunnel/internal/socks"
	"github.com/Kavun-Sama/salute-jazz-rtc-tunnel/internal/transport"
)

// Client runs a local SOCKS5 proxy and tunnels traffic through transport peers.
type Client struct {
	peers  []transport.Transport
	cipher *crypto.Cipher
	mx     *mux.Mux
	listen string

	clientID uint32
	nextSID  atomic.Uint32

	log *slog.Logger
}

// ClientConfig holds configuration for creating a tunnel client.
type ClientConfig struct {
	Peers    []transport.Transport
	Cipher   *crypto.Cipher
	Listen   string
	ClientID uint32
	Logger   *slog.Logger
}

// NewClient creates a new tunnel client.
func NewClient(cfg ClientConfig) *Client {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	c := &Client{
		peers:    cfg.Peers,
		cipher:   cfg.Cipher,
		listen:   cfg.Listen,
		clientID: cfg.ClientID,
		log:      cfg.Logger.With(slog.String("component", "tunnel/client")),
	}

	// Mux for client: no onStream callback since server doesn't initiate streams to us
	c.mx = mux.NewMux(c.sendFrame, nil, cfg.Logger)

	return c
}

// Run starts the SOCKS5 proxy and blocks until context is cancelled.
func (c *Client) Run(ctx context.Context) error {
	// Register data handler on all peers
	for _, p := range c.peers {
		p.SetOnData(func(data []byte) {
			plaintext, err := c.cipher.Decrypt(data)
			if err != nil {
				c.log.Warn("decrypt failed", "error", err)
				return
			}
			c.mx.HandleFrame(plaintext)
		})

		p.SetOnReconnect(func() {
			c.log.Info("peer reconnected, resetting mux state")
			c.mx.CloseAll()
			c.sendReset()
		})
	}

	// Send initial reset to clear stale server state
	c.sendReset()

	// Start SOCKS5 server
	socksServer := socks.NewServer(c.onConnect, c.log)

	errCh := make(chan error, 1)
	go func() {
		errCh <- socksServer.ListenAndServe(c.listen)
	}()

	select {
	case <-ctx.Done():
		socksServer.Close()
		c.mx.CloseAll()
		return nil
	case err := <-errCh:
		return fmt.Errorf("socks server: %w", err)
	}
}

func (c *Client) onConnect(conn net.Conn, host string, port int) {
	go c.handleConnect(conn, host, port)
}

func (c *Client) handleConnect(conn net.Conn, host string, port int) {
	sid := uint16(c.nextSID.Add(1))
	key := mux.StreamKey{ClientID: c.clientID, SID: sid}

	c.log.Debug("new connection",
		"host", host, "port", port,
		"clientID", c.clientID, "sid", sid,
	)

	stream := c.mx.OpenStream(key)

	// Send connect request
	req := ConnectRequest{
		Cmd:  cmdConnect,
		Addr: host,
		Port: port,
	}

	reqData, err := json.Marshal(req)
	if err != nil {
		c.log.Error("marshal connect request", "error", err)
		socks.SendFailure(conn)
		conn.Close()
		return
	}

	if err := stream.Write(reqData); err != nil {
		c.log.Error("send connect request", "error", err)
		socks.SendFailure(conn)
		conn.Close()
		c.mx.RemoveStream(key)
		return
	}

	// Wait for confirmation byte (0x00) from server
	confirmed := false
	timer := time.NewTimer(15 * time.Second)
	defer timer.Stop()

	firstFrameCh := make(chan []byte, 1)
	go func() {
		// stream.Close() on timeout unblocks Read(), so this helper goroutine does not leak.
		firstFrameCh <- stream.Read()
	}()

	select {
	case data := <-firstFrameCh:
		if data == nil {
			c.log.Debug("stream closed before confirmation")
			socks.SendFailure(conn)
			conn.Close()
			c.mx.RemoveStream(key)
			return
		}
		if len(data) == 1 && data[0] == 0x00 {
			confirmed = true
		}
	case <-timer.C:
		c.log.Warn("timeout waiting for connect confirmation", "host", host, "port", port)
		socks.SendFailure(conn)
		conn.Close()
		stream.Close()
		c.mx.RemoveStream(key)
		return
	}

	if !confirmed {
		socks.SendFailure(conn)
		conn.Close()
		stream.Close()
		c.mx.RemoveStream(key)
		return
	}

	// Send SOCKS5 success
	if err := socks.SendSuccess(conn); err != nil {
		c.log.Warn("send socks success", "error", err)
		conn.Close()
		stream.Close()
		c.mx.RemoveStream(key)
		return
	}

	c.log.Info("tunnel established",
		"host", host, "port", port,
		"sid", sid,
	)

	// Bidirectional proxy
	c.proxyStream(conn, stream, key)
}

func (c *Client) proxyStream(conn net.Conn, stream *mux.Stream, key mux.StreamKey) {
	var once sync.Once
	cleanup := func() {
		conn.Close()
		stream.Close()
		c.mx.RemoveStream(key)
	}

	done := make(chan struct{}, 2)

	// conn -> stream
	go func() {
		defer func() { done <- struct{}{} }()
		buf := make([]byte, 32*1024)
		for {
			n, err := conn.Read(buf)
			if n > 0 {
				if writeErr := stream.Write(buf[:n]); writeErr != nil {
					return
				}
			}
			if err != nil {
				return
			}
		}
	}()

	// stream -> conn
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

	// Wait for either side to finish
	<-done
	once.Do(cleanup)
	<-done
}

// sendFrame encrypts and sends through transport.
func (c *Client) sendFrame(data []byte) error {
	if len(c.peers) == 0 {
		return fmt.Errorf("no transport peers configured")
	}

	clientID, sid, _, flags, _, err := mux.ParseFrame(data)
	if err != nil {
		return fmt.Errorf("parse mux frame: %w", err)
	}

	if flags&mux.FlagReset != 0 {
		for _, peer := range c.peers {
			if err := sendEncryptedFrame(peer, c.cipher, data); err != nil {
				return err
			}
		}
		return nil
	}

	return sendEncryptedFrame(c.peers[streamPeerIndex(len(c.peers), clientID, sid)], c.cipher, data)
}

// sendReset sends a RESET frame to clear server-side state for this client.
func (c *Client) sendReset() {
	frame := mux.MakeFrame(c.clientID, 0, mux.FlagReset, nil)
	if err := c.sendFrame(frame); err != nil {
		c.log.Warn("send reset failed", "error", err)
	}
}

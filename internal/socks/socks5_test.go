package socks

import (
	"io"
	"log/slog"
	"net"
	"testing"
)

func TestNegotiateNoAuth(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	server := NewServer(func(net.Conn, string, int) {}, AuthConfig{}, slog.Default())

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.negotiate(serverConn)
	}()

	if _, err := clientConn.Write([]byte{socks5Version, 0x01, authMethodNone}); err != nil {
		t.Fatal(err)
	}

	reply := make([]byte, 2)
	if _, err := io.ReadFull(clientConn, reply); err != nil {
		t.Fatal(err)
	}
	if got := reply; got[0] != socks5Version || got[1] != authMethodNone {
		t.Fatalf("unexpected reply: %v", got)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("negotiate failed: %v", err)
	}
}

func TestNegotiateUserPassAuth(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	server := NewServer(func(net.Conn, string, int) {}, AuthConfig{
		Username: "alice",
		Password: "secret",
	}, slog.Default())

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.negotiate(serverConn)
	}()

	if _, err := clientConn.Write([]byte{socks5Version, 0x01, authMethodUserPass}); err != nil {
		t.Fatal(err)
	}

	reply := make([]byte, 2)
	if _, err := io.ReadFull(clientConn, reply); err != nil {
		t.Fatal(err)
	}
	if reply[0] != socks5Version || reply[1] != authMethodUserPass {
		t.Fatalf("unexpected method reply: %v", reply)
	}

	authReq := []byte{
		authVersion,
		byte(len("alice")),
	}
	authReq = append(authReq, []byte("alice")...)
	authReq = append(authReq, byte(len("secret")))
	authReq = append(authReq, []byte("secret")...)
	if _, err := clientConn.Write(authReq); err != nil {
		t.Fatal(err)
	}

	authReply := make([]byte, 2)
	if _, err := io.ReadFull(clientConn, authReply); err != nil {
		t.Fatal(err)
	}
	if authReply[0] != authVersion || authReply[1] != 0x00 {
		t.Fatalf("unexpected auth reply: %v", authReply)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("negotiate failed: %v", err)
	}
}

func TestNegotiateRejectsInvalidCredentials(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	server := NewServer(func(net.Conn, string, int) {}, AuthConfig{
		Username: "alice",
		Password: "secret",
	}, slog.Default())

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.negotiate(serverConn)
	}()

	if _, err := clientConn.Write([]byte{socks5Version, 0x01, authMethodUserPass}); err != nil {
		t.Fatal(err)
	}

	reply := make([]byte, 2)
	if _, err := io.ReadFull(clientConn, reply); err != nil {
		t.Fatal(err)
	}

	authReq := []byte{
		authVersion,
		byte(len("alice")),
	}
	authReq = append(authReq, []byte("alice")...)
	authReq = append(authReq, byte(len("wrong")))
	authReq = append(authReq, []byte("wrong")...)
	if _, err := clientConn.Write(authReq); err != nil {
		t.Fatal(err)
	}

	authReply := make([]byte, 2)
	if _, err := io.ReadFull(clientConn, authReply); err != nil {
		t.Fatal(err)
	}
	if authReply[0] != authVersion || authReply[1] != 0x01 {
		t.Fatalf("unexpected auth failure reply: %v", authReply)
	}
	if err := <-errCh; err == nil {
		t.Fatal("expected negotiate to fail")
	}
}

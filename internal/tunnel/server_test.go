package tunnel

import (
	"bytes"
	"io"
	"testing"
)

func TestReadSocksAuthResponse(t *testing.T) {
	if err := readSocksAuthResponse(bytes.NewReader([]byte{0x05, 0x00})); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestReadSocksAuthResponseShortRead(t *testing.T) {
	err := readSocksAuthResponse(bytes.NewReader([]byte{0x05}))
	if err == nil {
		t.Fatal("expected short-read error")
	}
	if err == io.EOF {
		t.Fatal("expected wrapped error, got bare EOF")
	}
}

func TestReadSocksConnectResponseIPv4(t *testing.T) {
	resp := []byte{
		0x05, 0x00, 0x00, 0x01,
		127, 0, 0, 1,
		0x1f, 0x90,
	}
	if err := readSocksConnectResponse(bytes.NewReader(resp)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestReadSocksConnectResponseDomain(t *testing.T) {
	resp := []byte{
		0x05, 0x00, 0x00, 0x03,
		0x0b,
		'e', 'x', 'a', 'm', 'p', 'l', 'e', '.', 'c', 'o', 'm',
		0x01, 0xbb,
	}
	if err := readSocksConnectResponse(bytes.NewReader(resp)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestReadSocksConnectResponseRejectsShortPayload(t *testing.T) {
	resp := []byte{
		0x05, 0x00, 0x00, 0x03,
		0x0b,
		'e', 'x', 'a',
	}
	if err := readSocksConnectResponse(bytes.NewReader(resp)); err == nil {
		t.Fatal("expected short payload error")
	}
}

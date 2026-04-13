package jazz

import (
	"bytes"
	"testing"

	livekit "github.com/livekit/protocol/livekit"
	"google.golang.org/protobuf/proto"
)

func TestEncodeDecodeRoundtrip(t *testing.T) {
	payload := []byte("hello, tunnel data")

	encoded, err := EncodeDataPacket(payload, nil)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeDataPacket(encoded)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(decoded, payload) {
		t.Fatalf("roundtrip mismatch: got %q, want %q", decoded, payload)
	}
}

func TestEncodeDecodeEmptyPayload(t *testing.T) {
	payload := []byte{}

	encoded, err := EncodeDataPacket(payload, nil)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeDataPacket(encoded)
	if err != nil {
		t.Fatal(err)
	}

	if len(decoded) != 0 {
		t.Fatalf("expected empty payload, got %d bytes", len(decoded))
	}
}

func TestEncodeDecodeLargePayload(t *testing.T) {
	payload := make([]byte, 16384)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	encoded, err := EncodeDataPacket(payload, nil)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeDataPacket(encoded)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(decoded, payload) {
		t.Fatal("large payload roundtrip mismatch")
	}
}

func TestDecodeEmpty(t *testing.T) {
	_, err := DecodeDataPacket([]byte{})
	if err == nil {
		t.Fatal("expected error for empty input")
	}
}

func TestDecodeInvalid(t *testing.T) {
	_, err := DecodeDataPacket([]byte{0xFF, 0xFF, 0xFF})
	if err == nil {
		t.Fatal("expected error for invalid protobuf data")
	}
}

func TestEncodeFormat(t *testing.T) {
	payload := []byte{0xAA, 0xBB}
	encoded, err := EncodeDataPacket(payload, nil)
	if err != nil {
		t.Fatal(err)
	}

	var packet livekit.DataPacket
	if err := proto.Unmarshal(encoded, &packet); err != nil {
		t.Fatal(err)
	}

	if packet.GetKind() != livekit.DataPacket_RELIABLE {
		t.Fatalf("unexpected packet kind: %v", packet.GetKind())
	}
	if !bytes.Equal(packet.GetUser().GetPayload(), payload) {
		t.Fatalf("unexpected payload: got %x, want %x", packet.GetUser().GetPayload(), payload)
	}
}

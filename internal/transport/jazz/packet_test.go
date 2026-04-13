package jazz

import (
	"bytes"
	"testing"
)

func TestEncodeDecodeRoundtrip(t *testing.T) {
	payload := []byte("hello, tunnel data")

	encoded := EncodeDataPacket(payload)
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

	encoded := EncodeDataPacket(payload)
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

	encoded := EncodeDataPacket(payload)
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
	encoded := EncodeDataPacket(payload)

	// Verify the outer structure:
	// 0x08 0x00 = field 1, varint, value 0 (RELIABLE)
	// 0x12 ...  = field 2, length-delimited (UserPacket)
	if len(encoded) < 4 {
		t.Fatal("encoded too short")
	}
	if encoded[0] != 0x08 || encoded[1] != 0x00 {
		t.Fatalf("expected kind field 0x08 0x00, got %x %x", encoded[0], encoded[1])
	}
	if encoded[2] != 0x12 {
		t.Fatalf("expected user packet tag 0x12, got %x", encoded[2])
	}
}

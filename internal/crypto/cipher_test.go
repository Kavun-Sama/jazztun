package crypto

import (
	"bytes"
	"crypto/rand"
	"testing"
)

func TestEncryptDecryptRoundtrip(t *testing.T) {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}

	c, err := NewCipher(key)
	if err != nil {
		t.Fatal(err)
	}

	plaintext := []byte("hello, world! this is a test message for AES-256-GCM encryption")

	encrypted, err := c.Encrypt(plaintext)
	if err != nil {
		t.Fatal(err)
	}

	if bytes.Equal(encrypted, plaintext) {
		t.Fatal("encrypted data should differ from plaintext")
	}

	decrypted, err := c.Decrypt(encrypted)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(decrypted, plaintext) {
		t.Fatalf("decrypted data mismatch: got %q, want %q", decrypted, plaintext)
	}
}

func TestDecryptWrongKey(t *testing.T) {
	key1 := make([]byte, 32)
	key2 := make([]byte, 32)
	if _, err := rand.Read(key1); err != nil {
		t.Fatal(err)
	}
	if _, err := rand.Read(key2); err != nil {
		t.Fatal(err)
	}

	c1, err := NewCipher(key1)
	if err != nil {
		t.Fatal(err)
	}

	c2, err := NewCipher(key2)
	if err != nil {
		t.Fatal(err)
	}

	encrypted, err := c1.Encrypt([]byte("secret data"))
	if err != nil {
		t.Fatal(err)
	}

	_, err = c2.Decrypt(encrypted)
	if err == nil {
		t.Fatal("expected error when decrypting with wrong key")
	}
}

func TestInvalidKeySize(t *testing.T) {
	_, err := NewCipher([]byte("too short"))
	if err == nil {
		t.Fatal("expected error for invalid key size")
	}
}

func TestDecryptTooShort(t *testing.T) {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}

	c, err := NewCipher(key)
	if err != nil {
		t.Fatal(err)
	}

	_, err = c.Decrypt([]byte("short"))
	if err == nil {
		t.Fatal("expected error for short ciphertext")
	}
}

func TestEncryptDecryptEmpty(t *testing.T) {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}

	c, err := NewCipher(key)
	if err != nil {
		t.Fatal(err)
	}

	encrypted, err := c.Encrypt([]byte{})
	if err != nil {
		t.Fatal(err)
	}

	decrypted, err := c.Decrypt(encrypted)
	if err != nil {
		t.Fatal(err)
	}

	if len(decrypted) != 0 {
		t.Fatalf("expected empty decrypted data, got %d bytes", len(decrypted))
	}
}

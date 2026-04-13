package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
)

const (
	nonceSize = 12 // AES-GCM standard nonce size
	keySize   = 32 // AES-256
)

// Cipher provides AES-256-GCM encryption and decryption.
// Encrypted frame format: [nonce 12 bytes][ciphertext...]
type Cipher struct {
	aead cipher.AEAD
}

// NewCipher creates a new AES-256-GCM cipher from a raw 32-byte key.
func NewCipher(key []byte) (*Cipher, error) {
	if len(key) != keySize {
		return nil, fmt.Errorf("invalid key size: got %d, want %d", len(key), keySize)
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("aes.NewCipher: %w", err)
	}

	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("cipher.NewGCM: %w", err)
	}

	return &Cipher{aead: aead}, nil
}

// Encrypt encrypts plaintext with AES-256-GCM.
// Returns [nonce 12 bytes][ciphertext+tag...].
func (c *Cipher) Encrypt(plaintext []byte) ([]byte, error) {
	nonce := make([]byte, nonceSize)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("generate nonce: %w", err)
	}

	// Seal appends ciphertext to nonce slice
	out := c.aead.Seal(nonce, nonce, plaintext, nil)
	return out, nil
}

// Decrypt decrypts data produced by Encrypt.
// Expects [nonce 12 bytes][ciphertext+tag...].
func (c *Cipher) Decrypt(data []byte) ([]byte, error) {
	if len(data) < nonceSize {
		return nil, errors.New("ciphertext too short")
	}

	nonce := data[:nonceSize]
	ciphertext := data[nonceSize:]

	plaintext, err := c.aead.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("decrypt: %w", err)
	}

	return plaintext, nil
}

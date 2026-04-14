package session

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

// DeriveID returns a stable, transport-safe session identifier.
// The optional namespace lets multiple independent jazztun pairs share one room.
func DeriveID(key []byte, namespace string) string {
	sum := sha256.Sum256([]byte("jazztun-session:" + strings.TrimSpace(namespace) + ":" + hex.EncodeToString(key)))
	return hex.EncodeToString(sum[:6])
}

// NewInstanceID returns a random identifier for one local transport instance.
func NewInstanceID() string {
	var raw [8]byte
	if _, err := rand.Read(raw[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(raw[:])
}

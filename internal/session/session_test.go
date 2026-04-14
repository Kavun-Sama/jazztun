package session

import "testing"

func TestDeriveIDStable(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")

	first := DeriveID(key, "")
	second := DeriveID(key, "")
	otherNamespace := DeriveID(key, "other")

	if first != second {
		t.Fatalf("session id not stable: %q != %q", first, second)
	}
	if first == otherNamespace {
		t.Fatalf("session id should change with namespace: %q", first)
	}
	if len(first) != 12 {
		t.Fatalf("unexpected session id length: got %d, want 12", len(first))
	}
}

package jazz

import "testing"

func TestPeerNameIncludesSession(t *testing.T) {
	got := peerName("client", "abc123def456", 1)
	want := "jazztun-client-abc123def456-2"
	if got != want {
		t.Fatalf("peerName() = %q, want %q", got, want)
	}
}

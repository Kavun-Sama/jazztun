package secure

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"testing"
	"time"

	icrypto "github.com/Kavun-Sama/jazztun/internal/crypto"
	"github.com/Kavun-Sama/jazztun/internal/transport"
)

type stubTransport struct {
	mu          sync.RWMutex
	onData      func([]byte)
	onReconnect func()
	peer        *stubTransport
	done        chan struct{}
}

func newStubPair() (*stubTransport, *stubTransport) {
	a := &stubTransport{done: make(chan struct{})}
	b := &stubTransport{done: make(chan struct{})}
	a.peer = b
	b.peer = a
	return a, b
}

func (s *stubTransport) Connect(ctx context.Context) error { return nil }
func (s *stubTransport) Send(data []byte) error {
	if s.peer == nil {
		return nil
	}
	s.peer.mu.RLock()
	fn := s.peer.onData
	s.peer.mu.RUnlock()
	if fn != nil {
		fn(append([]byte(nil), data...))
	}
	return nil
}
func (s *stubTransport) Close() error {
	select {
	case <-s.done:
	default:
		close(s.done)
	}
	return nil
}
func (s *stubTransport) Ready() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}
func (s *stubTransport) Done() <-chan struct{}  { return s.done }
func (s *stubTransport) CanSend() bool          { return true }
func (s *stubTransport) BufferedAmount() uint64 { return 0 }
func (s *stubTransport) SetOnData(fn func([]byte)) {
	s.mu.Lock()
	s.onData = fn
	s.mu.Unlock()
}
func (s *stubTransport) SetOnReconnect(fn func()) {
	s.mu.Lock()
	s.onReconnect = fn
	s.mu.Unlock()
}
func (s *stubTransport) triggerReconnect() {
	s.mu.RLock()
	fn := s.onReconnect
	s.mu.RUnlock()
	if fn != nil {
		fn()
	}
}

var _ transport.Transport = (*stubTransport)(nil)

func TestWrapAllAuthenticatesAndForwardsData(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
	cipher, err := icrypto.NewCipher(key)
	if err != nil {
		t.Fatal(err)
	}

	aRaw, bRaw := newStubPair()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := slog.New(slog.NewTextHandler(testWriter{t}, nil))
	server := New(ctx, aRaw, Config{
		Cipher:     cipher,
		SessionID:  "sess12345678",
		Role:       "server",
		PeerIndex:  0,
		InstanceID: "server1234",
		Logger:     logger,
	})
	client := New(ctx, bRaw, Config{
		Cipher:     cipher,
		SessionID:  "sess12345678",
		Role:       "client",
		PeerIndex:  0,
		InstanceID: "client1234",
		Logger:     logger,
	})

	waitFor(t, func() bool { return server.CanSend() && client.CanSend() }, 2*time.Second)

	got := make(chan []byte, 1)
	server.SetOnData(func(data []byte) {
		got <- data
	})

	payload := []byte("hello")
	if err := client.Send(payload); err != nil {
		t.Fatalf("send: %v", err)
	}

	select {
	case data := <-got:
		if string(data) != string(payload) {
			t.Fatalf("payload mismatch: got %q want %q", data, payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for forwarded payload")
	}
}

func TestHandleHelloRejectsDuplicateInstance(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
	cipher, err := icrypto.NewCipher(key)
	if err != nil {
		t.Fatal(err)
	}

	raw := &stubTransport{done: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tr := New(ctx, raw, Config{
		Cipher:     cipher,
		SessionID:  "sess12345678",
		Role:       "server",
		PeerIndex:  0,
		InstanceID: "server1234",
		Logger:     slog.Default(),
	})

	firstHello := mustHello(t, cipher, "client-a", helloPayload{Version: 1, Session: "sess12345678", Role: "client", PeerIndex: 0})
	secondHello := mustHello(t, cipher, "client-b", helloPayload{Version: 1, Session: "sess12345678", Role: "client", PeerIndex: 0})
	firstData := mustData(t, cipher, "client-a", []byte("good"))
	secondData := mustData(t, cipher, "client-b", []byte("bad"))

	got := make(chan []byte, 2)
	tr.SetOnData(func(data []byte) { got <- data })

	tr.handleIncoming(firstHello)
	tr.handleIncoming(secondHello)
	tr.handleIncoming(firstData)
	tr.handleIncoming(secondData)

	select {
	case data := <-got:
		if string(data) != "good" {
			t.Fatalf("unexpected forwarded payload %q", data)
		}
	case <-time.After(time.Second):
		t.Fatal("expected payload from selected instance")
	}

	select {
	case data := <-got:
		t.Fatalf("unexpected duplicate payload %q", data)
	case <-time.After(100 * time.Millisecond):
	}
}

func mustHello(t *testing.T, cipher *icrypto.Cipher, instance string, hello helloPayload) []byte {
	t.Helper()
	payload, err := json.Marshal(hello)
	if err != nil {
		t.Fatal(err)
	}
	packet, err := makeEnvelope(kindHello, instance, payload)
	if err != nil {
		t.Fatal(err)
	}
	out, err := cipher.Encrypt(packet)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func mustData(t *testing.T, cipher *icrypto.Cipher, instance string, payload []byte) []byte {
	t.Helper()
	packet, err := makeEnvelope(kindData, instance, payload)
	if err != nil {
		t.Fatal(err)
	}
	out, err := cipher.Encrypt(packet)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func waitFor(t *testing.T, fn func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(string(p))
	return len(p), nil
}

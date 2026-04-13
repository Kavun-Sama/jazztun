package mux

import (
	"sync"
	"testing"
	"time"
)

func TestDataFrameDelivered(t *testing.T) {
	var sent [][]byte
	var mu sync.Mutex

	sendFn := func(data []byte) error {
		mu.Lock()
		sent = append(sent, data)
		mu.Unlock()
		return nil
	}

	streamCh := make(chan *Stream, 1)
	m := NewMux(sendFn, func(s *Stream) {
		streamCh <- s
	}, nil)

	// Build a DATA frame for client 1, stream 5
	payload := []byte("hello mux")
	frame := MakeFrame(1, 5, FlagData, payload)

	m.HandleFrame(frame)

	select {
	case s := <-streamCh:
		if s.Key.ClientID != 1 || s.Key.SID != 5 {
			t.Fatalf("unexpected stream key: %+v", s.Key)
		}

		data := s.Read()
		if string(data) != "hello mux" {
			t.Fatalf("unexpected data: %q", data)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for stream")
	}
}

func TestDataRoutedToCorrectStream(t *testing.T) {
	sendFn := func(data []byte) error { return nil }

	streams := make(chan *Stream, 10)
	m := NewMux(sendFn, func(s *Stream) {
		streams <- s
	}, nil)

	// Create two different streams
	frame1 := MakeFrame(1, 1, FlagData, []byte("stream-1"))
	frame2 := MakeFrame(1, 2, FlagData, []byte("stream-2"))
	frame3 := MakeFrame(2, 1, FlagData, []byte("stream-3"))

	m.HandleFrame(frame1)
	m.HandleFrame(frame2)
	m.HandleFrame(frame3)

	// Collect all 3 new streams
	got := make(map[StreamKey]*Stream)
	for i := 0; i < 3; i++ {
		select {
		case s := <-streams:
			got[s.Key] = s
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for streams")
		}
	}

	// Verify data on each stream
	tests := []struct {
		key  StreamKey
		want string
	}{
		{StreamKey{1, 1}, "stream-1"},
		{StreamKey{1, 2}, "stream-2"},
		{StreamKey{2, 1}, "stream-3"},
	}

	for _, tt := range tests {
		s := got[tt.key]
		if s == nil {
			t.Fatalf("missing stream %+v", tt.key)
		}
		data := s.Read()
		if string(data) != tt.want {
			t.Fatalf("stream %+v: got %q, want %q", tt.key, data, tt.want)
		}
	}
}

func TestCloseFrameClosesStream(t *testing.T) {
	sendFn := func(data []byte) error { return nil }

	streamCh := make(chan *Stream, 1)
	m := NewMux(sendFn, func(s *Stream) {
		streamCh <- s
	}, nil)

	// First send a DATA frame to create the stream
	m.HandleFrame(MakeFrame(1, 1, FlagData, []byte("data")))

	var s *Stream
	select {
	case s = <-streamCh:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for stream")
	}

	// Read the data
	_ = s.Read()

	// Now send CLOSE frame
	m.HandleFrame(MakeFrame(1, 1, FlagClose, nil))

	// Channel should be closed
	data := s.Read()
	if data != nil {
		t.Fatalf("expected nil after close, got %q", data)
	}

	// Stream should be removed from mux
	if m.GetStream(StreamKey{1, 1}) != nil {
		t.Fatal("stream should be removed after close")
	}
}

func TestResetClosesAllClientStreams(t *testing.T) {
	sendFn := func(data []byte) error { return nil }

	var streamsMu sync.Mutex
	allStreams := make(map[StreamKey]*Stream)
	m := NewMux(sendFn, func(s *Stream) {
		streamsMu.Lock()
		allStreams[s.Key] = s
		streamsMu.Unlock()
	}, nil)

	// Create multiple streams for client 1
	m.HandleFrame(MakeFrame(1, 1, FlagData, []byte("a")))
	m.HandleFrame(MakeFrame(1, 2, FlagData, []byte("b")))
	m.HandleFrame(MakeFrame(1, 3, FlagData, []byte("c")))
	// And one for client 2
	m.HandleFrame(MakeFrame(2, 1, FlagData, []byte("d")))

	time.Sleep(50 * time.Millisecond) // let callbacks fire

	// Reset client 1
	m.HandleFrame(MakeFrame(1, 0, FlagReset, nil))

	time.Sleep(50 * time.Millisecond)

	// Client 1 streams should be closed
	streamsMu.Lock()
	for key, s := range allStreams {
		if key.ClientID == 1 {
			if !s.IsClosed() {
				t.Fatalf("stream %+v should be closed after reset", key)
			}
		}
	}
	streamsMu.Unlock()

	// Client 2 stream should still exist
	if m.GetStream(StreamKey{2, 1}) == nil {
		t.Fatal("client 2 stream should survive client 1 reset")
	}
}

func TestMakeFrameParseFrame(t *testing.T) {
	frame := MakeFrame(42, 7, FlagData, []byte("test"))

	clientID, sid, length, flags, payload, err := ParseFrame(frame)
	if err != nil {
		t.Fatal(err)
	}

	if clientID != 42 {
		t.Fatalf("clientID: got %d, want 42", clientID)
	}
	if sid != 7 {
		t.Fatalf("sid: got %d, want 7", sid)
	}
	if length != 4 {
		t.Fatalf("length: got %d, want 4", length)
	}
	if flags != FlagData {
		t.Fatalf("flags: got %x, want %x", flags, FlagData)
	}
	if string(payload) != "test" {
		t.Fatalf("payload: got %q, want %q", payload, "test")
	}
}

func TestStreamWrite(t *testing.T) {
	var sent [][]byte
	var mu sync.Mutex

	sendFn := func(data []byte) error {
		cp := make([]byte, len(data))
		copy(cp, data)
		mu.Lock()
		sent = append(sent, cp)
		mu.Unlock()
		return nil
	}

	m := NewMux(sendFn, nil, nil)
	s := m.OpenStream(StreamKey{ClientID: 1, SID: 10})

	err := s.Write([]byte("hello"))
	if err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(sent) != 1 {
		t.Fatalf("expected 1 frame sent, got %d", len(sent))
	}

	_, _, _, flags, payload, err := ParseFrame(sent[0])
	if err != nil {
		t.Fatal(err)
	}
	if flags != FlagData {
		t.Fatalf("expected DATA flag, got %x", flags)
	}
	if string(payload) != "hello" {
		t.Fatalf("expected payload 'hello', got %q", payload)
	}
}

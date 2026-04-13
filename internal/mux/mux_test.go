package mux

import (
	"encoding/binary"
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

	frame1 := MakeFrame(1, 1, FlagData, []byte("stream-1"))
	frame2 := MakeFrame(1, 2, FlagData, []byte("stream-2"))
	frame3 := MakeFrame(2, 1, FlagData, []byte("stream-3"))

	m.HandleFrame(frame1)
	m.HandleFrame(frame2)
	m.HandleFrame(frame3)

	got := make(map[StreamKey]*Stream)
	for i := 0; i < 3; i++ {
		select {
		case s := <-streams:
			got[s.Key] = s
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for streams")
		}
	}

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

	m.HandleFrame(MakeFrame(1, 1, FlagData, []byte("data")))

	var s *Stream
	select {
	case s = <-streamCh:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for stream")
	}

	_ = s.Read()

	m.HandleFrame(MakeFrame(1, 1, FlagClose, nil))

	if data := s.Read(); data != nil {
		t.Fatalf("expected nil after close, got %q", data)
	}
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

	m.HandleFrame(MakeFrame(1, 1, FlagData, []byte("a")))
	m.HandleFrame(MakeFrame(1, 2, FlagData, []byte("b")))
	m.HandleFrame(MakeFrame(1, 3, FlagData, []byte("c")))
	m.HandleFrame(MakeFrame(2, 1, FlagData, []byte("d")))

	time.Sleep(50 * time.Millisecond)

	m.HandleFrame(MakeFrame(1, 0, FlagReset, nil))

	time.Sleep(50 * time.Millisecond)

	streamsMu.Lock()
	for key, s := range allStreams {
		if key.ClientID == 1 && !s.IsClosed() {
			t.Fatalf("stream %+v should be closed after reset", key)
		}
	}
	streamsMu.Unlock()

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
	frames := make(chan []byte, 4)
	sendFn := func(data []byte) error {
		cp := make([]byte, len(data))
		copy(cp, data)
		frames <- cp
		return nil
	}

	m := NewMux(sendFn, nil, nil)
	s := m.OpenStream(StreamKey{ClientID: 1, SID: 10})

	if err := s.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}

	select {
	case frame := <-frames:
		_, _, _, flags, payload, err := ParseFrame(frame)
		if err != nil {
			t.Fatal(err)
		}
		if flags != FlagData {
			t.Fatalf("expected DATA flag, got %x", flags)
		}
		if string(payload) != "hello" {
			t.Fatalf("expected payload 'hello', got %q", payload)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for queued frame")
	}
}

func TestReadSendsWindowUpdate(t *testing.T) {
	frames := make(chan []byte, 4)
	sendFn := func(data []byte) error {
		cp := make([]byte, len(data))
		copy(cp, data)
		frames <- cp
		return nil
	}

	m := NewMux(sendFn, nil, nil)
	stream := m.OpenStream(StreamKey{ClientID: 1, SID: 11})

	m.HandleFrame(MakeFrame(1, 11, FlagData, []byte("abc")))

	if got := string(stream.Read()); got != "abc" {
		t.Fatalf("got %q, want abc", got)
	}

	select {
	case frame := <-frames:
		_, _, _, flags, payload, err := ParseFrame(frame)
		if err != nil {
			t.Fatal(err)
		}
		if flags != FlagWindowUpdate {
			t.Fatalf("expected WINDOW_UPDATE flag, got %x", flags)
		}
		if len(payload) != 4 || binary.BigEndian.Uint32(payload) != 3 {
			t.Fatalf("unexpected credit payload: %v", payload)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for window update")
	}
}

func TestWindowUpdateResumesSending(t *testing.T) {
	var (
		mu        sync.Mutex
		sentBytes int
	)

	sendFn := func(data []byte) error {
		_, _, _, flags, payload, err := ParseFrame(data)
		if err != nil {
			return err
		}
		if flags == FlagData {
			mu.Lock()
			sentBytes += len(payload)
			mu.Unlock()
		}
		return nil
	}

	m := NewMux(sendFn, nil, nil)
	stream := m.OpenStream(StreamKey{ClientID: 1, SID: 12})

	payload := make([]byte, initialSendWindow+maxChunk)
	if err := stream.Write(payload); err != nil {
		t.Fatal(err)
	}

	waitFor := func(want int) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			mu.Lock()
			got := sentBytes
			mu.Unlock()
			if got >= want {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		mu.Lock()
		got := sentBytes
		mu.Unlock()
		t.Fatalf("sent bytes: got %d, want at least %d", got, want)
	}

	waitFor(initialSendWindow)

	time.Sleep(100 * time.Millisecond)
	mu.Lock()
	before := sentBytes
	mu.Unlock()
	if before != initialSendWindow {
		t.Fatalf("expected scheduler to stop at window, got %d", before)
	}

	credit := make([]byte, 4)
	binary.BigEndian.PutUint32(credit, maxChunk)
	m.HandleFrame(MakeFrame(1, 12, FlagWindowUpdate, credit))

	waitFor(initialSendWindow + maxChunk)
}

func TestHandleDataBackpressureDoesNotDropFrames(t *testing.T) {
	sendFn := func(data []byte) error { return nil }

	m := NewMux(sendFn, nil, nil)
	key := StreamKey{ClientID: 7, SID: 9}
	stream := m.OpenStream(key)

	done := make(chan struct{})
	go func() {
		for i := 0; i < maxPendingFrames+1; i++ {
			payload := []byte{byte(i)}
			m.HandleFrame(MakeFrame(key.ClientID, key.SID, FlagData, payload))
		}
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("expected producer to block when stream queue is full")
	case <-time.After(100 * time.Millisecond):
	}

	if got := stream.Read(); len(got) != 1 || got[0] != 0 {
		t.Fatalf("unexpected first payload: %v", got)
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("producer did not resume after buffer space was freed")
	}

	for i := 1; i < maxPendingFrames+1; i++ {
		got := stream.Read()
		if len(got) != 1 || got[0] != byte(i) {
			t.Fatalf("payload %d: got %v", i, got)
		}
	}
}

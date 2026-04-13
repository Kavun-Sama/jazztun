package mux

import (
	"encoding/binary"
	"sync"
	"testing"
	"time"
)

func TestDataFrameDelivered(t *testing.T) {
	streamCh := make(chan *Stream, 1)
	m := NewMux(func([]byte) error { return nil }, func(s *Stream) {
		streamCh <- s
	}, nil)

	m.HandleFrame(makeDataFrame(1, 5, 0, []byte("hello mux")))

	select {
	case s := <-streamCh:
		if s.Key.ClientID != 1 || s.Key.SID != 5 {
			t.Fatalf("unexpected stream key: %+v", s.Key)
		}
		if got := string(s.Read()); got != "hello mux" {
			t.Fatalf("unexpected data: %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for stream")
	}
}

func TestDataRoutedToCorrectStream(t *testing.T) {
	streams := make(chan *Stream, 10)
	m := NewMux(func([]byte) error { return nil }, func(s *Stream) {
		streams <- s
	}, nil)

	m.HandleFrame(makeDataFrame(1, 1, 0, []byte("stream-1")))
	m.HandleFrame(makeDataFrame(1, 2, 0, []byte("stream-2")))
	m.HandleFrame(makeDataFrame(2, 1, 0, []byte("stream-3")))

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
		if got := string(s.Read()); got != tt.want {
			t.Fatalf("stream %+v: got %q, want %q", tt.key, got, tt.want)
		}
	}
}

func TestDuplicateFrameIsDeduped(t *testing.T) {
	streamCh := make(chan *Stream, 1)
	m := NewMux(func([]byte) error { return nil }, func(s *Stream) {
		streamCh <- s
	}, nil)

	key := StreamKey{ClientID: 9, SID: 4}
	m.HandleFrame(makeDataFrame(key.ClientID, key.SID, 0, []byte("once")))

	var stream *Stream
	select {
	case stream = <-streamCh:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for stream")
	}

	if got := string(stream.Read()); got != "once" {
		t.Fatalf("unexpected first payload: %q", got)
	}

	m.HandleFrame(makeDataFrame(key.ClientID, key.SID, 0, []byte("once")))

	readCh := make(chan []byte, 1)
	go func() {
		readCh <- stream.Read()
	}()

	select {
	case data := <-readCh:
		t.Fatalf("duplicate frame should not be delivered, got %q", data)
	case <-time.After(100 * time.Millisecond):
	}

	m.HandleFrame(MakeFrame(key.ClientID, key.SID, FlagClose, nil))

	select {
	case data := <-readCh:
		if data != nil {
			t.Fatalf("expected nil after close, got %q", data)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for blocked read to unwind")
	}
}

func TestCloseFrameClosesStream(t *testing.T) {
	streamCh := make(chan *Stream, 1)
	m := NewMux(func([]byte) error { return nil }, func(s *Stream) {
		streamCh <- s
	}, nil)

	m.HandleFrame(makeDataFrame(1, 1, 0, []byte("data")))

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
}

func TestResetClosesAllClientStreams(t *testing.T) {
	var streamsMu sync.Mutex
	allStreams := make(map[StreamKey]*Stream)
	m := NewMux(func([]byte) error { return nil }, func(s *Stream) {
		streamsMu.Lock()
		allStreams[s.Key] = s
		streamsMu.Unlock()
	}, nil)

	m.HandleFrame(makeDataFrame(1, 1, 0, []byte("a")))
	m.HandleFrame(makeDataFrame(1, 2, 0, []byte("b")))
	m.HandleFrame(makeDataFrame(1, 3, 0, []byte("c")))
	m.HandleFrame(makeDataFrame(2, 1, 0, []byte("d")))

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

func TestDataPayloadRoundTrip(t *testing.T) {
	payload := makeDataPayload(7, []byte("hello"))
	seq, data, err := parseDataPayload(payload)
	if err != nil {
		t.Fatal(err)
	}
	if seq != 7 {
		t.Fatalf("seq: got %d, want 7", seq)
	}
	if string(data) != "hello" {
		t.Fatalf("data: got %q, want hello", data)
	}
}

func TestStreamWrite(t *testing.T) {
	frames := make(chan []byte, 4)
	m := NewMux(func(data []byte) error {
		cp := make([]byte, len(data))
		copy(cp, data)
		frames <- cp
		return nil
	}, nil, nil)
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
		seq, data, err := parseDataPayload(payload)
		if err != nil {
			t.Fatal(err)
		}
		if seq != 0 {
			t.Fatalf("expected seq 0, got %d", seq)
		}
		if string(data) != "hello" {
			t.Fatalf("expected payload 'hello', got %q", data)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for queued frame")
	}
}

func TestReadSendsAckAndWindowUpdate(t *testing.T) {
	frames := make(chan []byte, 8)
	m := NewMux(func(data []byte) error {
		cp := make([]byte, len(data))
		copy(cp, data)
		frames <- cp
		return nil
	}, nil, nil)
	stream := m.OpenStream(StreamKey{ClientID: 1, SID: 11})

	m.HandleFrame(makeDataFrame(1, 11, 0, []byte("abc")))

	if got := string(stream.Read()); got != "abc" {
		t.Fatalf("got %q, want abc", got)
	}

	var sawAck, sawWindow bool
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && (!sawAck || !sawWindow) {
		select {
		case frame := <-frames:
			_, _, _, flags, payload, err := ParseFrame(frame)
			if err != nil {
				t.Fatal(err)
			}
			switch flags {
			case FlagAck:
				if len(payload) != ackPayloadSize || binary.BigEndian.Uint64(payload) != 1 {
					t.Fatalf("unexpected ack payload: %v", payload)
				}
				sawAck = true
			case FlagWindowUpdate:
				if len(payload) != 4 || binary.BigEndian.Uint32(payload) != 3 {
					t.Fatalf("unexpected credit payload: %v", payload)
				}
				sawWindow = true
			}
		case <-time.After(10 * time.Millisecond):
		}
	}

	if !sawAck {
		t.Fatal("expected ACK frame")
	}
	if !sawWindow {
		t.Fatal("expected WINDOW_UPDATE frame")
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
			_, chunk, err := parseDataPayload(payload)
			if err != nil {
				return err
			}
			mu.Lock()
			sentBytes += len(chunk)
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
	m := NewMux(func([]byte) error { return nil }, nil, nil)
	key := StreamKey{ClientID: 7, SID: 9}
	stream := m.OpenStream(key)

	done := make(chan struct{})
	go func() {
		for i := 0; i < maxPendingFrames+1; i++ {
			m.HandleFrame(makeDataFrame(key.ClientID, key.SID, uint64(i), []byte{byte(i)}))
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

func TestAckDropsUnackedAndReconnectReplays(t *testing.T) {
	frames := make(chan []byte, 8)
	m := NewMux(func(data []byte) error {
		cp := make([]byte, len(data))
		copy(cp, data)
		frames <- cp
		return nil
	}, nil, nil)

	key := StreamKey{ClientID: 3, SID: 2}
	stream := m.OpenStream(key)
	if err := stream.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}

	first := expectFrame(t, frames, FlagData)
	_, _, _, _, payload, err := ParseFrame(first)
	if err != nil {
		t.Fatal(err)
	}
	seq, data, err := parseDataPayload(payload)
	if err != nil {
		t.Fatal(err)
	}
	if seq != 0 || string(data) != "hello" {
		t.Fatalf("unexpected initial frame seq=%d data=%q", seq, data)
	}

	m.OnTransportReconnect()

	replayed := expectFrame(t, frames, FlagData)
	_, _, _, _, payload, err = ParseFrame(replayed)
	if err != nil {
		t.Fatal(err)
	}
	seq, data, err = parseDataPayload(payload)
	if err != nil {
		t.Fatal(err)
	}
	if seq != 0 || string(data) != "hello" {
		t.Fatalf("unexpected replayed frame seq=%d data=%q", seq, data)
	}

	ack := make([]byte, ackPayloadSize)
	binary.BigEndian.PutUint64(ack, 1)
	m.HandleFrame(MakeFrame(key.ClientID, key.SID, FlagAck, ack))
	m.OnTransportReconnect()

	select {
	case frame := <-frames:
		t.Fatalf("did not expect replay after ack, got frame %x", frame)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestCloseWaitsForAckedData(t *testing.T) {
	frames := make(chan []byte, 8)
	m := NewMux(func(data []byte) error {
		cp := make([]byte, len(data))
		copy(cp, data)
		frames <- cp
		return nil
	}, nil, nil)

	key := StreamKey{ClientID: 4, SID: 1}
	stream := m.OpenStream(key)
	if err := stream.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}

	dataFrame := expectFrame(t, frames, FlagData)
	_, _, _, _, payload, err := ParseFrame(dataFrame)
	if err != nil {
		t.Fatal(err)
	}
	seq, data, err := parseDataPayload(payload)
	if err != nil {
		t.Fatal(err)
	}
	if seq != 0 || string(data) != "hello" {
		t.Fatalf("unexpected data frame seq=%d data=%q", seq, data)
	}

	select {
	case frame := <-frames:
		t.Fatalf("close must wait for ack, got frame %x", frame)
	case <-time.After(100 * time.Millisecond):
	}

	ack := make([]byte, ackPayloadSize)
	binary.BigEndian.PutUint64(ack, 1)
	m.HandleFrame(MakeFrame(key.ClientID, key.SID, FlagAck, ack))

	closeFrame := expectFrame(t, frames, FlagClose)
	_, _, _, flags, payload, err := ParseFrame(closeFrame)
	if err != nil {
		t.Fatal(err)
	}
	if flags != FlagClose || len(payload) != 0 {
		t.Fatalf("unexpected close frame flags=%x payload=%v", flags, payload)
	}
}

func expectFrame(t *testing.T, frames <-chan []byte, wantFlag byte) []byte {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		select {
		case frame := <-frames:
			_, _, _, flags, _, err := ParseFrame(frame)
			if err != nil {
				t.Fatal(err)
			}
			if flags == wantFlag {
				return frame
			}
		case <-time.After(10 * time.Millisecond):
		}
	}
	t.Fatalf("timed out waiting for frame with flag %x", wantFlag)
	return nil
}

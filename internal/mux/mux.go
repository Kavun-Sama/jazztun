package mux

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"sync"
)

const (
	headerSize         = 9 // clientID(4) + sid(2) + length(2) + flags(1)
	maxChunk           = 16 * 1024
	maxPendingFrames   = 256
	maxPendingBytes    = 4 * 1024 * 1024
	maxQueuedSendBytes = 4 * 1024 * 1024
	initialSendWindow  = maxPendingBytes

	FlagData         byte = 0x01
	FlagClose        byte = 0x02
	FlagReset        byte = 0x04
	FlagWindowUpdate byte = 0x08
)

// StreamKey uniquely identifies a stream by client and stream ID.
type StreamKey struct {
	ClientID uint32
	SID      uint16
}

// Stream represents a single multiplexed stream.
type Stream struct {
	Key    StreamKey
	sendFn func([]byte) error
	mux    *Mux
	log    *slog.Logger

	mu sync.Mutex

	closed bool
	cond   *sync.Cond

	pendingIn      [][]byte
	pendingInBytes int

	sendQueue      [][]byte
	sendQueueBytes int
	sendWindow     int
}

func newStream(key StreamKey, sendFn func([]byte) error, parent *Mux, logger *slog.Logger) *Stream {
	s := &Stream{
		Key:        key,
		sendFn:     sendFn,
		mux:        parent,
		log:        logger.With("clientID", key.ClientID, "sid", key.SID),
		sendWindow: initialSendWindow,
	}
	s.cond = sync.NewCond(&s.mu)
	return s
}

// Read returns the next data chunk from the stream, blocking until available.
// Returns nil if the stream is closed.
func (s *Stream) Read() []byte {
	s.mu.Lock()
	for len(s.pendingIn) == 0 && !s.closed {
		s.cond.Wait()
	}

	if len(s.pendingIn) == 0 {
		s.mu.Unlock()
		return nil
	}

	data := s.pendingIn[0]
	s.pendingIn[0] = nil
	s.pendingIn = s.pendingIn[1:]
	s.pendingInBytes -= len(data)
	s.cond.Broadcast()
	s.mu.Unlock()

	if err := s.mux.sendWindowUpdate(s.Key, len(data)); err != nil {
		s.log.Debug("send window update failed", "error", err)
	}

	return data
}

func (s *Stream) enqueueIncoming(data []byte) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	for !s.closed && (len(s.pendingIn) >= maxPendingFrames || s.pendingInBytes+len(data) > maxPendingBytes) {
		s.cond.Wait()
	}
	if s.closed {
		return false
	}

	s.pendingIn = append(s.pendingIn, data)
	s.pendingInBytes += len(data)
	s.cond.Broadcast()
	return true
}

func (s *Stream) nextSendChunk() ([]byte, bool, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed || len(s.sendQueue) == 0 || s.sendWindow <= 0 {
		return nil, false, false
	}

	chunk := s.sendQueue[0]
	n := len(chunk)
	if n > s.sendWindow {
		n = s.sendWindow
	}
	sendChunk := chunk[:n]

	if n == len(chunk) {
		s.sendQueue[0] = nil
		s.sendQueue = s.sendQueue[1:]
	} else {
		s.sendQueue[0] = chunk[n:]
	}

	s.sendQueueBytes -= n
	s.sendWindow -= n
	s.cond.Broadcast()

	hasMore := len(s.sendQueue) > 0 && s.sendWindow > 0
	return sendChunk, hasMore, true
}

func (s *Stream) addSendWindow(credit int) bool {
	if credit <= 0 {
		return false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return false
	}

	s.sendWindow += credit
	s.cond.Broadcast()
	return len(s.sendQueue) > 0
}

func (s *Stream) closeLocal() {
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		s.cond.Broadcast()
	}
	s.mu.Unlock()
}

// Write queues data for sending through the mux.
// Large payloads are automatically chunked to maxChunk bytes.
func (s *Stream) Write(data []byte) error {
	for len(data) > 0 {
		chunkSize := len(data)
		if chunkSize > maxChunk {
			chunkSize = maxChunk
		}

		chunk := make([]byte, chunkSize)
		copy(chunk, data[:chunkSize])
		data = data[chunkSize:]

		s.mu.Lock()
		for !s.closed && s.sendQueueBytes+len(chunk) > maxQueuedSendBytes {
			s.cond.Wait()
		}
		if s.closed {
			s.mu.Unlock()
			return fmt.Errorf("stream %d/%d is closed", s.Key.ClientID, s.Key.SID)
		}

		s.sendQueue = append(s.sendQueue, chunk)
		s.sendQueueBytes += len(chunk)
		s.cond.Broadcast()
		s.mu.Unlock()

		s.mux.schedule(s.Key)
	}

	return nil
}

// Close sends a CLOSE frame and marks the stream as closed.
func (s *Stream) Close() error {
	s.closeLocal()

	frame := MakeFrame(s.Key.ClientID, s.Key.SID, FlagClose, nil)
	_ = s.sendFn(frame) // best-effort

	return nil
}

// IsClosed reports whether the stream has been closed.
func (s *Stream) IsClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// Mux is an event-driven multiplexer that routes frames to streams.
type Mux struct {
	streams  map[StreamKey]*Stream
	mu       sync.RWMutex
	sendFn   func([]byte) error
	onStream func(*Stream) // callback when new stream appears
	log      *slog.Logger

	schedMu   sync.Mutex
	scheduled map[StreamKey]bool
	scheduleQ []StreamKey
	wakeCh    chan struct{}
}

// NewMux creates a new multiplexer.
// sendFn is called to send frames to the transport.
// onStream is called when a new stream is created by an incoming frame.
func NewMux(sendFn func([]byte) error, onStream func(*Stream), logger *slog.Logger) *Mux {
	if logger == nil {
		logger = slog.Default()
	}
	m := &Mux{
		streams:   make(map[StreamKey]*Stream),
		sendFn:    sendFn,
		onStream:  onStream,
		log:       logger.With(slog.String("component", "mux")),
		scheduled: make(map[StreamKey]bool),
		wakeCh:    make(chan struct{}, 1),
	}
	go m.scheduleLoop()
	return m
}

// OpenStream creates a new outgoing stream with the given key.
func (m *Mux) OpenStream(key StreamKey) *Stream {
	m.mu.Lock()
	defer m.mu.Unlock()

	if s, ok := m.streams[key]; ok {
		return s
	}

	s := newStream(key, m.sendFn, m, m.log)
	m.streams[key] = s
	return s
}

// GetStream returns an existing stream or nil.
func (m *Mux) GetStream(key StreamKey) *Stream {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.streams[key]
}

// HandleFrame parses a mux frame and routes data to the correct stream.
func (m *Mux) HandleFrame(data []byte) {
	if len(data) < headerSize {
		m.log.Warn("frame too short", "len", len(data))
		return
	}

	clientID := binary.BigEndian.Uint32(data[0:4])
	sid := binary.BigEndian.Uint16(data[4:6])
	length := binary.BigEndian.Uint16(data[6:8])
	flags := data[8]

	payload := data[headerSize:]
	if len(payload) < int(length) {
		m.log.Warn("frame payload truncated", "expected", length, "got", len(payload))
		return
	}
	payload = payload[:length]

	key := StreamKey{ClientID: clientID, SID: sid}

	switch {
	case flags&FlagReset != 0:
		m.handleReset(clientID)
	case flags&FlagClose != 0:
		m.handleClose(key)
	case flags&FlagWindowUpdate != 0:
		m.handleWindowUpdate(key, payload)
	case flags&FlagData != 0:
		m.handleData(key, payload)
	default:
		m.log.Warn("unknown flags", "flags", flags)
	}
}

func (m *Mux) handleData(key StreamKey, payload []byte) {
	s, created := m.getOrCreateStream(key)
	if created && m.onStream != nil {
		m.onStream(s)
	}

	dataCopy := make([]byte, len(payload))
	copy(dataCopy, payload)

	if !s.enqueueIncoming(dataCopy) {
		m.log.Debug("dropping frame for closed stream", "clientID", key.ClientID, "sid", key.SID)
	}
}

func (m *Mux) handleWindowUpdate(key StreamKey, payload []byte) {
	if len(payload) != 4 {
		m.log.Warn("invalid window update payload", "len", len(payload))
		return
	}

	credit := int(binary.BigEndian.Uint32(payload))
	s := m.GetStream(key)
	if s == nil {
		return
	}
	if s.addSendWindow(credit) {
		m.schedule(key)
	}
}

func (m *Mux) getOrCreateStream(key StreamKey) (*Stream, bool) {
	m.mu.RLock()
	s, ok := m.streams[key]
	m.mu.RUnlock()
	if ok {
		return s, false
	}

	s = newStream(key, m.sendFn, m, m.log)

	m.mu.Lock()
	if existing, ok := m.streams[key]; ok {
		m.mu.Unlock()
		return existing, false
	}
	m.streams[key] = s
	m.mu.Unlock()
	return s, true
}

func (m *Mux) handleClose(key StreamKey) {
	m.mu.Lock()
	s, ok := m.streams[key]
	if ok {
		delete(m.streams, key)
	}
	m.mu.Unlock()

	if ok && s != nil {
		s.closeLocal()
	}
}

func (m *Mux) handleReset(clientID uint32) {
	m.log.Info("resetting streams for client", "clientID", clientID)

	m.mu.Lock()
	var toClose []*Stream
	for key, s := range m.streams {
		if key.ClientID == clientID {
			toClose = append(toClose, s)
			delete(m.streams, key)
		}
	}
	m.mu.Unlock()

	for _, s := range toClose {
		s.closeLocal()
	}
}

// CloseAll closes all streams and resets the mux state.
func (m *Mux) CloseAll() {
	m.mu.Lock()
	streams := m.streams
	m.streams = make(map[StreamKey]*Stream)
	m.mu.Unlock()

	for _, s := range streams {
		s.closeLocal()
	}
}

// RemoveStream removes a stream from the mux without sending a CLOSE frame.
func (m *Mux) RemoveStream(key StreamKey) {
	m.mu.Lock()
	s, ok := m.streams[key]
	if ok {
		delete(m.streams, key)
	}
	m.mu.Unlock()

	if ok && s != nil {
		s.closeLocal()
	}
}

func (m *Mux) schedule(key StreamKey) {
	m.schedMu.Lock()
	if !m.scheduled[key] {
		m.scheduled[key] = true
		m.scheduleQ = append(m.scheduleQ, key)
	}
	m.schedMu.Unlock()

	select {
	case m.wakeCh <- struct{}{}:
	default:
	}
}

func (m *Mux) popScheduled() (StreamKey, bool) {
	m.schedMu.Lock()
	defer m.schedMu.Unlock()

	if len(m.scheduleQ) == 0 {
		return StreamKey{}, false
	}

	key := m.scheduleQ[0]
	m.scheduleQ = m.scheduleQ[1:]
	delete(m.scheduled, key)
	return key, true
}

func (m *Mux) scheduleLoop() {
	for range m.wakeCh {
		for {
			key, ok := m.popScheduled()
			if !ok {
				break
			}

			stream := m.GetStream(key)
			if stream == nil {
				continue
			}

			chunk, hasMore, ok := stream.nextSendChunk()
			if !ok {
				continue
			}

			frame := MakeFrame(key.ClientID, key.SID, FlagData, chunk)
			if err := m.sendFn(frame); err != nil {
				m.log.Warn("send frame failed", "error", err, "clientID", key.ClientID, "sid", key.SID)
				stream.closeLocal()
				m.RemoveStream(key)
				continue
			}

			if hasMore {
				m.schedule(key)
			}
		}
	}
}

func (m *Mux) sendWindowUpdate(key StreamKey, credit int) error {
	if credit <= 0 {
		return nil
	}

	payload := make([]byte, 4)
	binary.BigEndian.PutUint32(payload, uint32(credit))
	frame := MakeFrame(key.ClientID, key.SID, FlagWindowUpdate, payload)
	return m.sendFn(frame)
}

// MakeFrame constructs a mux frame.
// Format: [clientID uint32][sid uint16][length uint16][flags uint8][data...]
func MakeFrame(clientID uint32, sid uint16, flags byte, data []byte) []byte {
	frame := make([]byte, headerSize+len(data))
	binary.BigEndian.PutUint32(frame[0:4], clientID)
	binary.BigEndian.PutUint16(frame[4:6], sid)
	binary.BigEndian.PutUint16(frame[6:8], uint16(len(data)))
	frame[8] = flags
	copy(frame[headerSize:], data)
	return frame
}

// ParseFrame parses a mux frame header without routing it.
func ParseFrame(data []byte) (clientID uint32, sid uint16, length uint16, flags byte, payload []byte, err error) {
	if len(data) < headerSize {
		return 0, 0, 0, 0, nil, fmt.Errorf("frame too short: %d bytes", len(data))
	}
	clientID = binary.BigEndian.Uint32(data[0:4])
	sid = binary.BigEndian.Uint16(data[4:6])
	length = binary.BigEndian.Uint16(data[6:8])
	flags = data[8]
	payload = data[headerSize:]
	if len(payload) < int(length) {
		return 0, 0, 0, 0, nil, fmt.Errorf("payload truncated: expected %d, got %d", length, len(payload))
	}
	payload = payload[:length]
	return clientID, sid, length, flags, payload, nil
}

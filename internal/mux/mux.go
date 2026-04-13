package mux

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"sync"
)

const (
	headerSize = 9 // clientID(4) + sid(2) + length(2) + flags(1)
	maxChunk   = 7000

	FlagData  byte = 0x01
	FlagClose byte = 0x02
	FlagReset byte = 0x04
)

// StreamKey uniquely identifies a stream by client and stream ID.
type StreamKey struct {
	ClientID uint32
	SID      uint16
}

// Stream represents a single multiplexed stream.
type Stream struct {
	Key    StreamKey
	inCh   chan []byte
	sendFn func([]byte) error
	closed bool
	mu     sync.Mutex
	log    *slog.Logger
}

// Read returns the next data chunk from the stream, blocking until available.
// Returns nil if the stream is closed.
func (s *Stream) Read() []byte {
	data, ok := <-s.inCh
	if !ok {
		return nil
	}
	return data
}

// ReadCh returns the channel for use in select statements.
func (s *Stream) ReadCh() <-chan []byte {
	return s.inCh
}

// Write sends data through the transport via the mux frame format.
// Large payloads are automatically chunked to maxChunk bytes.
func (s *Stream) Write(data []byte) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return fmt.Errorf("stream %d/%d is closed", s.Key.ClientID, s.Key.SID)
	}
	s.mu.Unlock()

	for len(data) > 0 {
		chunk := data
		if len(chunk) > maxChunk {
			chunk = data[:maxChunk]
		}
		data = data[len(chunk):]

		frame := MakeFrame(s.Key.ClientID, s.Key.SID, FlagData, chunk)
		if err := s.sendFn(frame); err != nil {
			return fmt.Errorf("send frame: %w", err)
		}
	}
	return nil
}

// Close sends a CLOSE frame and marks the stream as closed.
func (s *Stream) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()

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
}

// NewMux creates a new multiplexer.
// sendFn is called to send frames to the transport.
// onStream is called when a new stream is created by an incoming frame.
func NewMux(sendFn func([]byte) error, onStream func(*Stream), logger *slog.Logger) *Mux {
	if logger == nil {
		logger = slog.Default()
	}
	return &Mux{
		streams:  make(map[StreamKey]*Stream),
		sendFn:   sendFn,
		onStream: onStream,
		log:      logger.With(slog.String("component", "mux")),
	}
}

// OpenStream creates a new outgoing stream with the given key.
func (m *Mux) OpenStream(key StreamKey) *Stream {
	m.mu.Lock()
	defer m.mu.Unlock()

	if s, ok := m.streams[key]; ok {
		return s
	}

	s := &Stream{
		Key:    key,
		inCh:   make(chan []byte, 256),
		sendFn: m.sendFn,
		log:    m.log.With("clientID", key.ClientID, "sid", key.SID),
	}
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
		return

	case flags&FlagClose != 0:
		m.handleClose(key)
		return

	case flags&FlagData != 0:
		m.handleData(key, payload)
		return

	default:
		m.log.Warn("unknown flags", "flags", flags)
	}
}

func (m *Mux) handleData(key StreamKey, payload []byte) {
	m.mu.RLock()
	s, ok := m.streams[key]
	m.mu.RUnlock()

	if !ok {
		// New stream from remote side
		s = &Stream{
			Key:    key,
			inCh:   make(chan []byte, 256),
			sendFn: m.sendFn,
			log:    m.log.With("clientID", key.ClientID, "sid", key.SID),
		}

		m.mu.Lock()
		// Double-check
		if existing, ok := m.streams[key]; ok {
			s = existing
		} else {
			m.streams[key] = s
		}
		m.mu.Unlock()

		if s.Key == key && m.onStream != nil {
			m.onStream(s)
		}
	}

	// Non-blocking send to channel; drop if full (backpressure)
	dataCopy := make([]byte, len(payload))
	copy(dataCopy, payload)

	select {
	case s.inCh <- dataCopy:
	default:
		m.log.Warn("stream buffer full, dropping frame", "clientID", key.ClientID, "sid", key.SID)
	}
}

func (m *Mux) handleClose(key StreamKey) {
	m.mu.Lock()
	s, ok := m.streams[key]
	if ok {
		delete(m.streams, key)
	}
	m.mu.Unlock()

	if ok && s != nil {
		s.mu.Lock()
		if !s.closed {
			s.closed = true
			close(s.inCh)
		}
		s.mu.Unlock()
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
		s.mu.Lock()
		if !s.closed {
			s.closed = true
			close(s.inCh)
		}
		s.mu.Unlock()
	}
}

// CloseAll closes all streams and resets the mux state.
func (m *Mux) CloseAll() {
	m.mu.Lock()
	streams := m.streams
	m.streams = make(map[StreamKey]*Stream)
	m.mu.Unlock()

	for _, s := range streams {
		s.mu.Lock()
		if !s.closed {
			s.closed = true
			close(s.inCh)
		}
		s.mu.Unlock()
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
		s.mu.Lock()
		if !s.closed {
			s.closed = true
			close(s.inCh)
		}
		s.mu.Unlock()
	}
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

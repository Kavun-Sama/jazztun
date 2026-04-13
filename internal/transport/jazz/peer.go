package jazz

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v4"
)

const (
	maxBufferedAmount    = 512 * 1024 // 512 KB backpressure threshold
	pingInterval         = 5 * time.Second
	reconnectMaxAttempts = 10
	reconnectWindow      = 5 * time.Minute
	reconnectBaseDelay   = 2 * time.Second
	reconnectMaxDelay    = 30 * time.Second
	dcLabel              = "_reliable"
)

// WSMessage is the Jazz WebSocket message envelope.
type WSMessage struct {
	Event     string          `json:"event"`
	RoomID    string          `json:"roomId,omitempty"`
	GroupID   string          `json:"groupId,omitempty"`
	RequestID string          `json:"requestId,omitempty"`
	Payload   json.RawMessage `json:"payload"`
}

// Peer implements the Transport interface using Jazz/LiveKit WebRTC.
type Peer struct {
	roomID       string
	password     string
	connectorURL string
	apiClient    *APIClient

	ws        *websocket.Conn
	wsMu      sync.Mutex
	pc        *webrtc.PeerConnection
	dc        *webrtc.DataChannel
	groupID   string
	participantSID string

	onData      func([]byte)
	onDataMu    sync.RWMutex
	onReconnect func()
	onReconnMu  sync.RWMutex

	readyCh    chan struct{}
	doneCh     chan struct{}
	closeCh    chan struct{}
	closed     atomic.Bool

	iceServers []webrtc.ICEServer

	log *slog.Logger
}

// PeerConfig holds configuration for creating a new Peer.
type PeerConfig struct {
	RoomID       string
	Password     string
	ConnectorURL string
	APIClient    *APIClient
	Logger       *slog.Logger
}

// NewPeer creates a new Jazz transport peer.
func NewPeer(cfg PeerConfig) *Peer {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Peer{
		roomID:       cfg.RoomID,
		password:     cfg.Password,
		connectorURL: cfg.ConnectorURL,
		apiClient:    cfg.APIClient,
		readyCh:      make(chan struct{}),
		doneCh:       make(chan struct{}),
		closeCh:      make(chan struct{}),
		log:          cfg.Logger.With(slog.String("component", "jazz/peer")),
	}
}

// Connect establishes the WebRTC connection through Jazz signaling.
func (p *Peer) Connect(ctx context.Context) error {
	p.log.Info("connecting", "roomId", p.roomID)

	if err := p.connectWS(ctx); err != nil {
		return fmt.Errorf("websocket connect: %w", err)
	}

	if err := p.join(); err != nil {
		p.closeWS()
		return fmt.Errorf("join: %w", err)
	}

	// Process messages until DC is ready or context cancelled
	go p.readLoop()

	select {
	case <-p.readyCh:
		p.log.Info("data channel ready")
		return nil
	case <-p.doneCh:
		return fmt.Errorf("connection closed before data channel ready")
	case <-ctx.Done():
		p.Close()
		return ctx.Err()
	}
}

// Send sends data through the data channel, wrapped in a LiveKit DataPacket.
func (p *Peer) Send(data []byte) error {
	if p.closed.Load() {
		return fmt.Errorf("peer is closed")
	}
	if p.dc == nil {
		return fmt.Errorf("data channel not ready")
	}

	packet := EncodeDataPacket(data)
	return p.dc.Send(packet)
}

// Close tears down the peer connection.
func (p *Peer) Close() error {
	if !p.closed.CompareAndSwap(false, true) {
		return nil
	}

	close(p.closeCh)

	p.sendLeave()
	p.closeWS()

	if p.dc != nil {
		p.dc.Close()
	}
	if p.pc != nil {
		p.pc.Close()
	}

	// Signal done if not already
	select {
	case <-p.doneCh:
	default:
		close(p.doneCh)
	}

	p.log.Info("peer closed")
	return nil
}

// Ready returns a channel that closes when the data channel is open.
func (p *Peer) Ready() <-chan struct{} {
	return p.readyCh
}

// Done returns a channel that closes when the peer disconnects.
func (p *Peer) Done() <-chan struct{} {
	return p.doneCh
}

// CanSend checks backpressure on the data channel.
func (p *Peer) CanSend() bool {
	if p.dc == nil {
		return false
	}
	return p.dc.BufferedAmount() < maxBufferedAmount
}

// SetOnData registers a callback for incoming data channel messages.
func (p *Peer) SetOnData(fn func([]byte)) {
	p.onDataMu.Lock()
	p.onData = fn
	p.onDataMu.Unlock()
}

// SetOnReconnect registers a callback for reconnection events.
func (p *Peer) SetOnReconnect(fn func()) {
	p.onReconnMu.Lock()
	p.onReconnect = fn
	p.onReconnMu.Unlock()
}

// WatchConnection monitors the connection and attempts reconnection on failure.
func (p *Peer) WatchConnection(ctx context.Context) {
	attempts := 0
	windowStart := time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		case <-p.doneCh:
			if p.closed.Load() {
				return
			}
		}

		// Reset attempt window
		if time.Since(windowStart) > reconnectWindow {
			attempts = 0
			windowStart = time.Now()
		}

		if attempts >= reconnectMaxAttempts {
			p.log.Error("max reconnect attempts reached")
			return
		}

		delay := reconnectBaseDelay
		for i := 0; i < attempts; i++ {
			delay *= 2
			if delay > reconnectMaxDelay {
				delay = reconnectMaxDelay
				break
			}
		}

		p.log.Info("reconnecting", "attempt", attempts+1, "delay", delay)

		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return
		}

		attempts++

		// Re-preconnect
		preResp, err := p.apiClient.Preconnect(p.roomID, p.password)
		if err != nil {
			p.log.Error("preconnect failed", "error", err)
			continue
		}
		p.connectorURL = preResp.ConnectorURL

		// Reset channels for new connection
		p.doneCh = make(chan struct{})
		p.readyCh = make(chan struct{})
		p.closeCh = make(chan struct{})
		p.closed.Store(false)

		if err := p.Connect(ctx); err != nil {
			p.log.Error("reconnect failed", "error", err)
			continue
		}

		p.log.Info("reconnected successfully")

		p.onReconnMu.RLock()
		fn := p.onReconnect
		p.onReconnMu.RUnlock()
		if fn != nil {
			fn()
		}

		// Reset attempts on success
		attempts = 0
		windowStart = time.Now()
	}
}

func (p *Peer) connectWS(ctx context.Context) error {
	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}

	headers := map[string][]string{
		"Origin":     {origin},
		"User-Agent": {userAgent},
	}

	ws, _, err := dialer.DialContext(ctx, p.connectorURL, headers)
	if err != nil {
		return fmt.Errorf("dial websocket: %w", err)
	}

	p.wsMu.Lock()
	p.ws = ws
	p.wsMu.Unlock()

	p.log.Debug("websocket connected", "url", p.connectorURL)
	return nil
}

func (p *Peer) closeWS() {
	p.wsMu.Lock()
	defer p.wsMu.Unlock()
	if p.ws != nil {
		p.ws.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		p.ws.Close()
		p.ws = nil
	}
}

func (p *Peer) join() error {
	payload := map[string]any{
		"password":        p.password,
		"participantName": "olcrtc",
		"supportedFeatures": map[string]any{
			"attachedRooms":  true,
			"sessionGroups":  true,
			"transcription":  true,
		},
		"isSilent": false,
	}

	return p.sendWS("join", payload)
}

func (p *Peer) sendLeave() {
	p.sendWS("leave", map[string]any{})
}

func (p *Peer) sendWS(event string, payload any) error {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	msg := WSMessage{
		Event:     event,
		RoomID:    p.roomID,
		GroupID:   p.groupID,
		RequestID: uuid.New().String(),
		Payload:   payloadBytes,
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}

	p.wsMu.Lock()
	defer p.wsMu.Unlock()

	if p.ws == nil {
		return fmt.Errorf("websocket not connected")
	}

	return p.ws.WriteMessage(websocket.TextMessage, data)
}

func (p *Peer) readLoop() {
	defer func() {
		select {
		case <-p.doneCh:
		default:
			close(p.doneCh)
		}
	}()

	// Start ping loop once we have the connection
	go p.pingLoop()

	for {
		select {
		case <-p.closeCh:
			return
		default:
		}

		p.wsMu.Lock()
		ws := p.ws
		p.wsMu.Unlock()

		if ws == nil {
			return
		}

		_, data, err := ws.ReadMessage()
		if err != nil {
			if !p.closed.Load() {
				p.log.Warn("websocket read error", "error", err)
			}
			return
		}

		var msg WSMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			p.log.Warn("unmarshal ws message", "error", err)
			continue
		}

		p.handleMessage(msg)
	}
}

func (p *Peer) handleMessage(msg WSMessage) {
	switch msg.Event {
	case "join-response":
		p.handleJoinResponse(msg.Payload)
	case "media-out":
		p.handleMediaOut(msg.Payload)
	case "leave-response":
		p.log.Debug("leave response received")
	default:
		p.log.Debug("unhandled event", "event", msg.Event)
	}
}

func (p *Peer) handleJoinResponse(payload json.RawMessage) {
	var resp struct {
		RoomID    string `json:"roomId"`
		MeetingID string `json:"meetingId"`
		Participant struct {
			SessionID     string `json:"sessionId"`
			ParticipantID string `json:"participantId"`
		} `json:"participant"`
		ParticipantGroup struct {
			GroupID string `json:"groupId"`
			Type    string `json:"type"`
		} `json:"participantGroup"`
	}

	if err := json.Unmarshal(payload, &resp); err != nil {
		p.log.Error("unmarshal join-response", "error", err)
		return
	}

	p.groupID = resp.ParticipantGroup.GroupID
	p.log.Info("joined room",
		"roomId", resp.RoomID,
		"groupId", p.groupID,
		"participantId", resp.Participant.ParticipantID,
	)
}

func (p *Peer) handleMediaOut(payload json.RawMessage) {
	var media struct {
		Method        string          `json:"method"`
		Configuration json.RawMessage `json:"configuration"`
		Join          json.RawMessage `json:"join"`
		Description   json.RawMessage `json:"description"`
		IceCandidates json.RawMessage `json:"rtcIceCandidates"`
		PingReq       json.RawMessage `json:"ping_req"`
	}

	if err := json.Unmarshal(payload, &media); err != nil {
		p.log.Warn("unmarshal media-out", "error", err)
		return
	}

	switch media.Method {
	case "rtc:config":
		p.handleRTCConfig(media.Configuration)
	case "rtc:join":
		p.handleRTCJoin(media.Join)
	case "rtc:offer":
		p.handleRTCOffer(media.Description)
	case "rtc:ice":
		p.handleRemoteICE(media.IceCandidates)
	case "rtc:ping":
		p.handleRTCPing(media.PingReq)
	default:
		p.log.Debug("unhandled media method", "method", media.Method)
	}
}

func (p *Peer) handleRTCConfig(data json.RawMessage) {
	var config struct {
		ICEServers []struct {
			URLs       []string `json:"urls"`
			Username   string   `json:"username"`
			Credential string   `json:"credential"`
		} `json:"iceServers"`
	}

	if err := json.Unmarshal(data, &config); err != nil {
		p.log.Error("unmarshal rtc:config", "error", err)
		return
	}

	p.iceServers = nil
	for _, s := range config.ICEServers {
		p.iceServers = append(p.iceServers, webrtc.ICEServer{
			URLs:       s.URLs,
			Username:   s.Username,
			Credential: s.Credential,
		})
	}

	p.log.Debug("ICE servers configured", "count", len(p.iceServers))
}

func (p *Peer) handleRTCJoin(data json.RawMessage) {
	var join struct {
		Participant struct {
			SID string `json:"sid"`
		} `json:"participant"`
		Room struct {
			SID string `json:"sid"`
		} `json:"room"`
		PingInterval int `json:"pingInterval"`
		PingTimeout  int `json:"pingTimeout"`
	}

	if err := json.Unmarshal(data, &join); err != nil {
		p.log.Error("unmarshal rtc:join", "error", err)
		return
	}

	p.participantSID = join.Participant.SID
	p.log.Info("rtc:join received",
		"participantSID", p.participantSID,
		"roomSID", join.Room.SID,
	)
}

func (p *Peer) handleRTCOffer(data json.RawMessage) {
	var desc struct {
		Type string `json:"type"`
		SDP  string `json:"sdp"`
	}

	if err := json.Unmarshal(data, &desc); err != nil {
		p.log.Error("unmarshal rtc:offer", "error", err)
		return
	}

	p.log.Debug("received SDP offer")

	if err := p.setupPeerConnection(desc.SDP); err != nil {
		p.log.Error("setup peer connection", "error", err)
		return
	}
}

func (p *Peer) setupPeerConnection(offerSDP string) error {
	config := webrtc.Configuration{
		ICEServers: p.iceServers,
	}

	pc, err := webrtc.NewPeerConnection(config)
	if err != nil {
		return fmt.Errorf("new peer connection: %w", err)
	}
	p.pc = pc

	// Handle incoming data channels
	pc.OnDataChannel(func(dc *webrtc.DataChannel) {
		p.log.Debug("data channel received", "label", dc.Label())

		if dc.Label() != dcLabel {
			return
		}

		dc.OnOpen(func() {
			p.log.Info("data channel open", "label", dc.Label())
			p.dc = dc

			select {
			case <-p.readyCh:
			default:
				close(p.readyCh)
			}
		})

		dc.OnMessage(func(msg webrtc.DataChannelMessage) {
			payload, err := DecodeDataPacket(msg.Data)
			if err != nil {
				p.log.Warn("decode data packet", "error", err)
				return
			}

			p.onDataMu.RLock()
			fn := p.onData
			p.onDataMu.RUnlock()

			if fn != nil {
				fn(payload)
			}
		})

		dc.OnClose(func() {
			p.log.Warn("data channel closed")
		})
	})

	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return
		}
		p.sendICECandidate(c)
	})

	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		p.log.Info("peer connection state", "state", state.String())
		switch state {
		case webrtc.PeerConnectionStateFailed, webrtc.PeerConnectionStateDisconnected, webrtc.PeerConnectionStateClosed:
			select {
			case <-p.doneCh:
			default:
				close(p.doneCh)
			}
		}
	})

	// Set remote description (offer)
	offer := webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP:  offerSDP,
	}
	if err := pc.SetRemoteDescription(offer); err != nil {
		return fmt.Errorf("set remote description: %w", err)
	}

	// Create and set answer
	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		return fmt.Errorf("create answer: %w", err)
	}
	if err := pc.SetLocalDescription(answer); err != nil {
		return fmt.Errorf("set local description: %w", err)
	}

	// Send answer
	p.sendMediaIn("rtc:answer", map[string]any{
		"description": map[string]any{
			"type": answer.Type.String(),
			"sdp":  answer.SDP,
		},
	})

	return nil
}

func (p *Peer) sendICECandidate(c *webrtc.ICECandidate) {
	init := c.ToJSON()

	candidate := map[string]any{
		"candidate":     init.Candidate,
		"sdpMLineIndex": 0,
		"sdpMid":        "0",
		"target":        "SUBSCRIBER",
	}

	if init.UsernameFragment != nil {
		candidate["usernameFragment"] = *init.UsernameFragment
	}

	p.sendMediaIn("rtc:ice", map[string]any{
		"rtcIceCandidates": []any{candidate},
	})
}

func (p *Peer) handleRemoteICE(data json.RawMessage) {
	if p.pc == nil {
		p.log.Warn("received ICE candidate before peer connection")
		return
	}

	var candidates []struct {
		Candidate        string `json:"candidate"`
		SDPMLineIndex    *uint16 `json:"sdpMLineIndex"`
		SDPMid           *string `json:"sdpMid"`
		UsernameFragment string  `json:"usernameFragment"`
	}

	if err := json.Unmarshal(data, &candidates); err != nil {
		p.log.Warn("unmarshal ICE candidates", "error", err)
		return
	}

	for _, c := range candidates {
		init := webrtc.ICECandidateInit{
			Candidate:     c.Candidate,
			SDPMLineIndex: c.SDPMLineIndex,
			SDPMid:        c.SDPMid,
		}
		if err := p.pc.AddICECandidate(init); err != nil {
			p.log.Warn("add ICE candidate", "error", err)
		}
	}
}

func (p *Peer) handleRTCPing(data json.RawMessage) {
	var ping struct {
		Timestamp int64 `json:"timestamp"`
		RTT       int64 `json:"rtt"`
	}

	if err := json.Unmarshal(data, &ping); err != nil {
		p.log.Debug("unmarshal rtc:ping", "error", err)
		return
	}

	now := time.Now().UnixMilli()
	p.sendMediaIn("rtc:pong", map[string]any{
		"pong_resp": map[string]any{
			"lastPingTimestamp": fmt.Sprintf("%d", ping.Timestamp),
			"timestamp":        fmt.Sprintf("%d", now),
		},
	})
}

func (p *Peer) sendMediaIn(method string, extra map[string]any) {
	payload := map[string]any{
		"method": method,
	}
	for k, v := range extra {
		payload[k] = v
	}
	p.sendWS("media-in", payload)
}

func (p *Peer) pingLoop() {
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.closeCh:
			return
		case <-p.doneCh:
			return
		case <-ticker.C:
			now := time.Now().UnixMilli()
			p.sendMediaIn("rtc:ping", map[string]any{
				"ping_req": map[string]any{
					"timestamp": now,
					"rtt":       0,
				},
			})
		}
	}
}

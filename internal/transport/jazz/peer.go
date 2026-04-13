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
	maxBufferedAmount    = 4 * 1024 * 1024 // 4 MB backpressure threshold
	pingInterval         = 5 * time.Second
	reconnectMaxAttempts = 10
	reconnectWindow      = 5 * time.Minute
	reconnectBaseDelay   = 2 * time.Second
	reconnectMaxDelay    = 30 * time.Second
	targetReadyTimeout   = 45 * time.Second
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
	roomID                string
	password              string
	connectorURL          string
	apiClient             *APIClient
	participantName       string
	targetParticipantName string

	ws      *websocket.Conn
	wsMu    sync.Mutex
	pc      *webrtc.PeerConnection
	dc      *webrtc.DataChannel
	pubPC   *webrtc.PeerConnection
	pubDC   *webrtc.DataChannel
	groupID string

	participantSID      string
	participantIdentity string

	remoteIdentities   map[string]struct{}
	remoteIdentitiesMu sync.RWMutex

	onData      func([]byte)
	onDataMu    sync.RWMutex
	onReconnect func()
	onReconnMu  sync.RWMutex

	readyCh       chan struct{}
	doneCh        chan struct{}
	closeCh       chan struct{}
	closed        atomic.Bool
	targetReadyCh chan struct{}

	iceServers []webrtc.ICEServer
	pubReadyCh chan struct{}

	pubTrackMu          sync.Mutex
	pubTrackCID         string
	pubTrackPublishedCh chan struct{}

	log *slog.Logger
}

// PeerConfig holds configuration for creating a new Peer.
type PeerConfig struct {
	RoomID                string
	Password              string
	ConnectorURL          string
	APIClient             *APIClient
	ParticipantName       string
	TargetParticipantName string
	Logger                *slog.Logger
}

// NewPeer creates a new Jazz transport peer.
func NewPeer(cfg PeerConfig) *Peer {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Peer{
		roomID:                cfg.RoomID,
		password:              cfg.Password,
		connectorURL:          cfg.ConnectorURL,
		apiClient:             cfg.APIClient,
		participantName:       cfg.ParticipantName,
		targetParticipantName: cfg.TargetParticipantName,
		remoteIdentities:      make(map[string]struct{}),
		readyCh:               make(chan struct{}),
		pubReadyCh:            make(chan struct{}),
		doneCh:                make(chan struct{}),
		closeCh:               make(chan struct{}),
		targetReadyCh:         make(chan struct{}),
		log:                   cfg.Logger.With(slog.String("component", "jazz/peer")),
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
		if err := p.startPublisher(ctx); err != nil {
			p.log.Warn("publisher setup failed", "error", err)
		} else {
			select {
			case <-p.pubReadyCh:
				p.log.Info("publisher data channel ready")
				if p.targetParticipantName != "" {
					select {
					case <-p.targetReadyCh:
						p.log.Info("target participant ready", "name", p.targetParticipantName)
					case <-time.After(targetReadyTimeout):
						return fmt.Errorf("target participant %q not discovered", p.targetParticipantName)
					case <-ctx.Done():
						return ctx.Err()
					}
				}
			case <-time.After(10 * time.Second):
				p.log.Warn("publisher data channel not ready yet")
			case <-ctx.Done():
				return ctx.Err()
			}
		}
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
	dc := p.sendDataChannel()
	if dc == nil {
		return fmt.Errorf("data channel not ready")
	}

	packet, err := EncodeDataPacket(data, p.destinationIdentities())
	if err != nil {
		return err
	}
	return dc.Send(packet)
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
	if p.pubDC != nil {
		p.pubDC.Close()
	}
	if p.pc != nil {
		p.pc.Close()
	}
	if p.pubPC != nil {
		p.pubPC.Close()
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
	dc := p.sendDataChannel()
	if dc == nil {
		return false
	}
	if p.targetParticipantName != "" && len(p.destinationIdentities()) == 0 {
		return false
	}
	return dc.BufferedAmount() < maxBufferedAmount
}

// BufferedAmount reports the buffered byte count of the least-loaded send path.
func (p *Peer) BufferedAmount() uint64 {
	dc := p.sendDataChannel()
	if dc == nil {
		return ^uint64(0)
	}
	return dc.BufferedAmount()
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
			p.log.Warn("preconnect failed, reusing default connector", "error", err, "connectorUrl", DefaultConnectorURL)
			p.connectorURL = DefaultConnectorURL
		} else {
			p.connectorURL = preResp.ConnectorURL
		}

		// Reset channels for new connection
		p.doneCh = make(chan struct{})
		p.readyCh = make(chan struct{})
		p.pubReadyCh = make(chan struct{})
		p.closeCh = make(chan struct{})
		p.targetReadyCh = make(chan struct{})
		p.closed.Store(false)
		p.pubPC = nil
		p.pubDC = nil

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
	participantName := p.participantName
	if participantName == "" {
		participantName = "jazztun"
	}

	payload := map[string]any{
		"password":        p.password,
		"participantName": participantName,
		"supportedFeatures": map[string]any{
			"attachedRooms": true,
			"sessionGroups": true,
			"transcription": true,
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
	case "participant-joined":
		p.handleParticipantJoined(msg.Payload)
	case "participant-left":
		p.handleParticipantLeft(msg.Payload)
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
		RoomID      string `json:"roomId"`
		MeetingID   string `json:"meetingId"`
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
		Method                 string          `json:"method"`
		Configuration          json.RawMessage `json:"configuration"`
		Join                   json.RawMessage `json:"join"`
		TrackPublishedResponse json.RawMessage `json:"trackPublishedResponse"`
		Update                 json.RawMessage `json:"update"`
		Description            json.RawMessage `json:"description"`
		IceCandidates          json.RawMessage `json:"rtcIceCandidates"`
		PingReq                json.RawMessage `json:"ping_req"`
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
	case "rtc:participants:update":
		p.handleParticipantsUpdate(media.Update)
	case "rtc:track:published":
		p.handleTrackPublished(media.TrackPublishedResponse)
	case "rtc:offer":
		p.handleRTCOffer(media.Description)
	case "rtc:answer":
		p.handleRTCAnswer(media.Description)
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
			SID      string `json:"sid"`
			Identity string `json:"identity"`
			Name     string `json:"name"`
		} `json:"participant"`
		OtherParticipants []struct {
			SID      string `json:"sid"`
			Identity string `json:"identity"`
			Name     string `json:"name"`
			State    string `json:"state"`
		} `json:"otherParticipants"`
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
	p.participantIdentity = join.Participant.Identity
	for _, other := range join.OtherParticipants {
		p.updateRemoteIdentity(other.Identity, other.Name, other.State)
	}

	p.log.Info("rtc:join received",
		"participantSID", p.participantSID,
		"participantIdentity", p.participantIdentity,
		"roomSID", join.Room.SID,
	)
}

func (p *Peer) handleParticipantsUpdate(data json.RawMessage) {
	var update struct {
		Participants []struct {
			SID      string `json:"sid"`
			Identity string `json:"identity"`
			Name     string `json:"name"`
			State    string `json:"state"`
		} `json:"participants"`
	}

	if err := json.Unmarshal(data, &update); err != nil {
		p.log.Warn("unmarshal rtc:participants:update", "error", err)
		return
	}

	for _, participant := range update.Participants {
		p.updateRemoteIdentity(participant.Identity, participant.Name, participant.State)
	}
}

func (p *Peer) handleTrackPublished(data json.RawMessage) {
	var resp struct {
		CID string `json:"cid"`
	}

	if err := json.Unmarshal(data, &resp); err != nil {
		p.log.Warn("unmarshal rtc:track:published", "error", err)
		return
	}

	p.pubTrackMu.Lock()
	defer p.pubTrackMu.Unlock()

	if resp.CID != "" && resp.CID == p.pubTrackCID && p.pubTrackPublishedCh != nil {
		select {
		case <-p.pubTrackPublishedCh:
		default:
			close(p.pubTrackPublishedCh)
		}
	}
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

func (p *Peer) handleRTCAnswer(data json.RawMessage) {
	var desc struct {
		Type string `json:"type"`
		SDP  string `json:"sdp"`
	}

	if err := json.Unmarshal(data, &desc); err != nil {
		p.log.Error("unmarshal rtc:answer", "error", err)
		return
	}

	if p.pubPC == nil {
		p.log.Warn("received publisher answer before publisher peer connection")
		return
	}

	answer := webrtc.SessionDescription{
		Type: webrtc.SDPTypeAnswer,
		SDP:  desc.SDP,
	}
	if err := p.pubPC.SetRemoteDescription(answer); err != nil {
		p.log.Error("set publisher remote description", "error", err)
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
			p.handleDataChannelMessage(msg)
		})

		dc.OnClose(func() {
			p.log.Warn("data channel closed")
		})
	})

	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return
		}
		p.sendICECandidate(c, "SUBSCRIBER")
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

func (p *Peer) sendICECandidate(c *webrtc.ICECandidate, target string) {
	init := c.ToJSON()

	candidate := map[string]any{
		"candidate":     init.Candidate,
		"sdpMLineIndex": 0,
		"sdpMid":        "0",
		"target":        target,
	}

	if init.UsernameFragment != nil {
		candidate["usernameFragment"] = *init.UsernameFragment
	}

	p.sendMediaIn("rtc:ice", map[string]any{
		"rtcIceCandidates": []any{candidate},
	})
}

func (p *Peer) handleRemoteICE(data json.RawMessage) {
	var candidates []struct {
		Candidate        string  `json:"candidate"`
		SDPMLineIndex    *uint16 `json:"sdpMLineIndex"`
		SDPMid           *string `json:"sdpMid"`
		UsernameFragment string  `json:"usernameFragment"`
		Target           string  `json:"target"`
	}

	if err := json.Unmarshal(data, &candidates); err != nil {
		p.log.Warn("unmarshal ICE candidates", "error", err)
		return
	}

	for _, c := range candidates {
		var pc *webrtc.PeerConnection
		switch c.Target {
		case "PUBLISHER":
			pc = p.pubPC
		default:
			pc = p.pc
		}
		if pc == nil {
			p.log.Warn("received ICE candidate before peer connection", "target", c.Target)
			continue
		}

		init := webrtc.ICECandidateInit{
			Candidate:     c.Candidate,
			SDPMLineIndex: c.SDPMLineIndex,
			SDPMid:        c.SDPMid,
		}
		if err := pc.AddICECandidate(init); err != nil {
			p.log.Warn("add ICE candidate", "error", err, "target", c.Target)
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
			"timestamp":         fmt.Sprintf("%d", now),
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

func (p *Peer) updateRemoteIdentity(identity, name, state string) {
	if identity == "" || identity == p.participantIdentity {
		return
	}

	p.remoteIdentitiesMu.Lock()
	defer p.remoteIdentitiesMu.Unlock()

	if p.targetParticipantName != "" {
		if state != "DISCONNECTED" && name == "" {
			return
		}
		if name != "" && name != p.targetParticipantName {
			delete(p.remoteIdentities, identity)
			return
		}
	}

	switch state {
	case "DISCONNECTED":
		delete(p.remoteIdentities, identity)
	default:
		p.remoteIdentities[identity] = struct{}{}
		if p.targetParticipantName == "" || name == p.targetParticipantName {
			select {
			case <-p.targetReadyCh:
			default:
				close(p.targetReadyCh)
			}
		}
	}
}

func (p *Peer) destinationIdentities() []string {
	p.remoteIdentitiesMu.RLock()
	defer p.remoteIdentitiesMu.RUnlock()

	if len(p.remoteIdentities) == 0 {
		return nil
	}

	identities := make([]string, 0, len(p.remoteIdentities))
	for identity := range p.remoteIdentities {
		identities = append(identities, identity)
	}
	return identities
}

func (p *Peer) sendDataChannel() *webrtc.DataChannel {
	if p.pubDC != nil && p.pubDC.ReadyState() == webrtc.DataChannelStateOpen {
		return p.pubDC
	}
	if p.dc != nil && p.dc.ReadyState() == webrtc.DataChannelStateOpen {
		return p.dc
	}
	return nil
}

func (p *Peer) handleParticipantJoined(payload json.RawMessage) {
	var event struct {
		ParticipantID   string `json:"participantId"`
		ParticipantName string `json:"participantName"`
	}

	if err := json.Unmarshal(payload, &event); err != nil {
		p.log.Debug("unmarshal participant-joined", "error", err)
		return
	}

	p.updateRemoteIdentity(event.ParticipantID, event.ParticipantName, "JOINED")
}

func (p *Peer) handleParticipantLeft(payload json.RawMessage) {
	var event struct {
		ParticipantID   string `json:"participantId"`
		ParticipantName string `json:"participantName"`
	}

	if err := json.Unmarshal(payload, &event); err != nil {
		p.log.Debug("unmarshal participant-left", "error", err)
		return
	}

	p.updateRemoteIdentity(event.ParticipantID, event.ParticipantName, "DISCONNECTED")
}

func (p *Peer) startPublisher(ctx context.Context) error {
	if p.pubPC != nil {
		return nil
	}

	config := webrtc.Configuration{
		ICEServers: p.iceServers,
	}

	pc, err := webrtc.NewPeerConnection(config)
	if err != nil {
		return fmt.Errorf("new publisher peer connection: %w", err)
	}
	p.pubPC = pc

	trueVal := true
	falseVal := false
	maxRetries := uint16(1)

	lossyDC, err := pc.CreateDataChannel("_lossy", &webrtc.DataChannelInit{
		Ordered:        &falseVal,
		MaxRetransmits: &maxRetries,
	})
	if err != nil {
		return fmt.Errorf("create lossy publisher datachannel: %w", err)
	}
	lossyDC.OnMessage(p.handleDataChannelMessage)

	reliableDC, err := pc.CreateDataChannel("_reliable", &webrtc.DataChannelInit{
		Ordered: &trueVal,
	})
	if err != nil {
		return fmt.Errorf("create reliable publisher datachannel: %w", err)
	}
	reliableDC.OnOpen(func() {
		p.log.Info("publisher data channel open", "label", reliableDC.Label())
		p.pubDC = reliableDC
		select {
		case <-p.pubReadyCh:
		default:
			close(p.pubReadyCh)
		}
	})
	reliableDC.OnMessage(p.handleDataChannelMessage)

	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return
		}
		p.sendICECandidate(c, "PUBLISHER")
	})

	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		p.log.Info("publisher peer connection state", "state", state.String())
	})

	trackCID := "{" + uuid.New().String() + "}"
	track, err := webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2},
		trackCID,
		"jazztun",
	)
	if err != nil {
		return fmt.Errorf("create publisher track: %w", err)
	}

	sender, err := pc.AddTrack(track)
	if err != nil {
		return fmt.Errorf("add publisher track: %w", err)
	}

	go func() {
		buf := make([]byte, 1500)
		for {
			if _, _, err := sender.Read(buf); err != nil {
				return
			}
		}
	}()

	p.pubTrackMu.Lock()
	p.pubTrackCID = trackCID
	p.pubTrackPublishedCh = make(chan struct{})
	p.pubTrackMu.Unlock()

	p.sendMediaIn("rtc:track:add", map[string]any{
		"addTrackRequest": map[string]any{
			"cid":    trackCID,
			"type":   "AUDIO",
			"source": "MICROPHONE",
			"muted":  true,
		},
	})

	select {
	case <-p.pubTrackPublishedCh:
	case <-time.After(10 * time.Second):
		return fmt.Errorf("timeout waiting for rtc:track:published")
	case <-ctx.Done():
		return ctx.Err()
	}

	offer, err := pc.CreateOffer(nil)
	if err != nil {
		return fmt.Errorf("create publisher offer: %w", err)
	}
	if err := pc.SetLocalDescription(offer); err != nil {
		return fmt.Errorf("set publisher local description: %w", err)
	}

	p.sendMediaIn("rtc:offer", map[string]any{
		"description": map[string]any{
			"type": offer.Type.String(),
			"sdp":  offer.SDP,
		},
	})

	return nil
}

func (p *Peer) handleDataChannelMessage(msg webrtc.DataChannelMessage) {
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
}

package secure

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	icrypto "github.com/Kavun-Sama/jazztun/internal/crypto"
	"github.com/Kavun-Sama/jazztun/internal/session"
	"github.com/Kavun-Sama/jazztun/internal/transport"
)

const (
	envelopeMagic = "JTZ2"
	kindHello     = 1
	kindData      = 2
	helloInterval = 1 * time.Second
)

type Config struct {
	Cipher     *icrypto.Cipher
	SessionID  string
	Role       string
	PeerIndex  int
	InstanceID string
	Logger     *slog.Logger
}

type helloPayload struct {
	Version   int    `json:"version"`
	Session   string `json:"session"`
	Role      string `json:"role"`
	PeerIndex int    `json:"peerIndex"`
}

type envelope struct {
	Kind     byte
	Instance string
	Payload  []byte
}

type Transport struct {
	inner transport.Transport

	cipher       *icrypto.Cipher
	sessionID    string
	role         string
	expectedRole string
	peerIndex    int
	instanceID   string
	log          *slog.Logger
	authWakeCh   chan struct{}

	mu                sync.RWMutex
	onData            func([]byte)
	onReconnect       func()
	remoteInstanceID  string
	authenticated     bool
	reconnectPending  bool
	lastDuplicateSeen string
}

func WrapAll(ctx context.Context, peers []transport.Transport, cfg Config) []transport.Transport {
	wrapped := make([]transport.Transport, len(peers))
	instanceID := cfg.InstanceID
	if instanceID == "" {
		instanceID = session.NewInstanceID()
	}
	for i, peer := range peers {
		peerCfg := cfg
		peerCfg.PeerIndex = i
		peerCfg.InstanceID = instanceID
		wrapped[i] = New(ctx, peer, peerCfg)
	}
	return wrapped
}

func New(ctx context.Context, inner transport.Transport, cfg Config) *Transport {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	t := &Transport{
		inner:        inner,
		cipher:       cfg.Cipher,
		sessionID:    cfg.SessionID,
		role:         cfg.Role,
		expectedRole: oppositeRole(cfg.Role),
		peerIndex:    cfg.PeerIndex,
		instanceID:   cfg.InstanceID,
		log: cfg.Logger.With(
			slog.String("component", "transport/secure"),
			slog.Int("peerIndex", cfg.PeerIndex+1),
			slog.String("session", cfg.SessionID),
		),
		authWakeCh: make(chan struct{}, 1),
	}

	inner.SetOnData(t.handleIncoming)
	inner.SetOnReconnect(t.handleReconnect)

	go t.authLoop(ctx)
	t.kickAuth()

	return t
}

func (t *Transport) Connect(ctx context.Context) error {
	return t.inner.Connect(ctx)
}

func (t *Transport) Send(data []byte) error {
	if !t.isAuthenticated() {
		return fmt.Errorf("transport session not authenticated")
	}

	packet, err := makeEnvelope(kindData, t.instanceID, data)
	if err != nil {
		return err
	}
	ciphertext, err := t.cipher.Encrypt(packet)
	if err != nil {
		return fmt.Errorf("encrypt secure envelope: %w", err)
	}
	return t.inner.Send(ciphertext)
}

func (t *Transport) Close() error {
	return t.inner.Close()
}

func (t *Transport) Ready() <-chan struct{} {
	return t.inner.Ready()
}

func (t *Transport) Done() <-chan struct{} {
	return t.inner.Done()
}

func (t *Transport) CanSend() bool {
	return t.isAuthenticated() && t.inner.CanSend()
}

func (t *Transport) BufferedAmount() uint64 {
	if !t.isAuthenticated() {
		return ^uint64(0)
	}
	return t.inner.BufferedAmount()
}

func (t *Transport) SetOnData(fn func([]byte)) {
	t.mu.Lock()
	t.onData = fn
	t.mu.Unlock()
}

func (t *Transport) SetOnReconnect(fn func()) {
	t.mu.Lock()
	t.onReconnect = fn
	t.mu.Unlock()
}

func (t *Transport) handleReconnect() {
	t.mu.Lock()
	t.authenticated = false
	t.remoteInstanceID = ""
	t.reconnectPending = true
	t.lastDuplicateSeen = ""
	t.mu.Unlock()

	t.kickAuth()
}

func (t *Transport) handleIncoming(data []byte) {
	plaintext, err := t.cipher.Decrypt(data)
	if err != nil {
		t.log.Debug("drop transport packet: outer decrypt failed", "error", err)
		return
	}

	env, err := parseEnvelope(plaintext)
	if err != nil {
		t.log.Debug("drop transport packet: invalid envelope", "error", err)
		return
	}

	switch env.Kind {
	case kindHello:
		t.handleHello(env)
	case kindData:
		t.handleData(env)
	default:
		t.log.Debug("drop transport packet: unknown kind", "kind", env.Kind)
	}
}

func (t *Transport) handleHello(env envelope) {
	var hello helloPayload
	if err := json.Unmarshal(env.Payload, &hello); err != nil {
		t.log.Debug("drop auth hello: invalid payload", "error", err)
		return
	}
	if hello.Version != 1 {
		t.log.Debug("drop auth hello: unsupported version", "version", hello.Version)
		return
	}
	if hello.Session != t.sessionID {
		t.log.Debug("drop auth hello: wrong session", "session", hello.Session)
		return
	}
	if hello.Role != t.expectedRole {
		t.log.Debug("drop auth hello: wrong role", "role", hello.Role)
		return
	}
	if hello.PeerIndex != t.peerIndex {
		t.log.Debug("drop auth hello: wrong peer index", "peerIndex", hello.PeerIndex+1)
		return
	}

	var (
		notifyReconnect bool
		reconnectFn     func()
	)

	t.mu.Lock()
	switch {
	case t.remoteInstanceID == "":
		t.remoteInstanceID = env.Instance
	case t.remoteInstanceID != env.Instance:
		if t.lastDuplicateSeen != env.Instance {
			t.lastDuplicateSeen = env.Instance
			t.log.Warn("duplicate remote session detected; ignoring extra participant",
				"expectedInstance", t.remoteInstanceID,
				"duplicateInstance", env.Instance,
			)
		}
		t.mu.Unlock()
		return
	}

	if !t.authenticated {
		t.authenticated = true
		t.log.Info("transport peer authenticated", "remoteInstance", env.Instance)
	}
	if t.reconnectPending {
		t.reconnectPending = false
		notifyReconnect = true
		reconnectFn = t.onReconnect
	}
	t.mu.Unlock()

	if notifyReconnect && reconnectFn != nil {
		reconnectFn()
	}
}

func (t *Transport) handleData(env envelope) {
	t.mu.RLock()
	authenticated := t.authenticated
	remoteInstanceID := t.remoteInstanceID
	onData := t.onData
	t.mu.RUnlock()

	if !authenticated || env.Instance != remoteInstanceID || onData == nil {
		return
	}

	onData(env.Payload)
}

func (t *Transport) authLoop(ctx context.Context) {
	ticker := time.NewTicker(helloInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.Done():
			if ctx.Err() != nil {
				return
			}
		case <-ticker.C:
		case <-t.authWakeCh:
		}

		if !t.inner.CanSend() {
			continue
		}

		if err := t.sendHello(); err != nil {
			t.log.Debug("send auth hello failed", "error", err)
		}
	}
}

func (t *Transport) sendHello() error {
	payload, err := json.Marshal(helloPayload{
		Version:   1,
		Session:   t.sessionID,
		Role:      t.role,
		PeerIndex: t.peerIndex,
	})
	if err != nil {
		return fmt.Errorf("marshal auth hello: %w", err)
	}

	packet, err := makeEnvelope(kindHello, t.instanceID, payload)
	if err != nil {
		return err
	}
	ciphertext, err := t.cipher.Encrypt(packet)
	if err != nil {
		return fmt.Errorf("encrypt auth hello: %w", err)
	}
	return t.inner.Send(ciphertext)
}

func (t *Transport) kickAuth() {
	select {
	case t.authWakeCh <- struct{}{}:
	default:
	}
}

func (t *Transport) isAuthenticated() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.authenticated
}

func makeEnvelope(kind byte, instance string, payload []byte) ([]byte, error) {
	if len(instance) == 0 || len(instance) > 255 {
		return nil, fmt.Errorf("invalid instance id length %d", len(instance))
	}

	out := make([]byte, 0, len(envelopeMagic)+2+len(instance)+len(payload))
	out = append(out, envelopeMagic...)
	out = append(out, kind)
	out = append(out, byte(len(instance)))
	out = append(out, []byte(instance)...)
	out = append(out, payload...)
	return out, nil
}

func parseEnvelope(data []byte) (envelope, error) {
	if len(data) < len(envelopeMagic)+2 {
		return envelope{}, fmt.Errorf("envelope too short")
	}
	if string(data[:len(envelopeMagic)]) != envelopeMagic {
		return envelope{}, fmt.Errorf("invalid magic")
	}
	kind := data[len(envelopeMagic)]
	instanceLen := int(data[len(envelopeMagic)+1])
	start := len(envelopeMagic) + 2
	if len(data) < start+instanceLen {
		return envelope{}, fmt.Errorf("invalid instance length")
	}
	return envelope{
		Kind:     kind,
		Instance: string(data[start : start+instanceLen]),
		Payload:  append([]byte(nil), data[start+instanceLen:]...),
	}, nil
}

func oppositeRole(role string) string {
	if role == "client" {
		return "server"
	}
	return "client"
}

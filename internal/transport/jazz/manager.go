package jazz

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/Kavun-Sama/jazztun/internal/transport"
	"golang.org/x/sync/errgroup"
)

// Manager provisions and connects transport peers for one room.
type Manager struct {
	apiClient    *APIClient
	room         RoomSpec
	peersPerRoom int
	sessionID    string
	role         string
	log          *slog.Logger
}

// ManagerConfig describes how to build a room/peer topology.
type ManagerConfig struct {
	APIClient    *APIClient
	Room         RoomSpec
	PeersPerRoom int
	SessionID    string
	Role         string
	Logger       *slog.Logger
}

// NewManager creates a transport manager for one side of the tunnel.
func NewManager(cfg ManagerConfig) (*Manager, error) {
	if cfg.APIClient == nil {
		return nil, fmt.Errorf("api client is required")
	}
	if cfg.Room.RoomID == "" {
		return nil, fmt.Errorf("room is required")
	}
	if cfg.PeersPerRoom <= 0 {
		return nil, fmt.Errorf("peers per room must be > 0")
	}
	if cfg.SessionID == "" {
		return nil, fmt.Errorf("session id is required")
	}
	if cfg.Role != "client" && cfg.Role != "server" {
		return nil, fmt.Errorf("invalid role %q", cfg.Role)
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	return &Manager{
		apiClient:    cfg.APIClient,
		room:         cfg.Room,
		peersPerRoom: cfg.PeersPerRoom,
		sessionID:    cfg.SessionID,
		role:         cfg.Role,
		log:          cfg.Logger.With(slog.String("component", "jazz/manager")),
	}, nil
}

// ConnectAll creates, connects, and starts reconnection loops for all peers.
func (m *Manager) ConnectAll(ctx context.Context) ([]transport.Transport, error) {
	connectorURL := m.room.ConnectorURL
	if connectorURL == "" {
		preResp, err := m.apiClient.Preconnect(m.room.RoomID, m.room.Password)
		if err != nil {
			m.log.Warn("preconnect failed, using default connector",
				"error", err,
				"roomId", m.room.RoomID,
				"connectorUrl", DefaultConnectorURL,
			)
			connectorURL = DefaultConnectorURL
		} else {
			connectorURL = preResp.ConnectorURL
		}
	}

	peers := make([]transport.Transport, m.peersPerRoom)
	group, groupCtx := errgroup.WithContext(ctx)

	for peerIndex := 0; peerIndex < m.peersPerRoom; peerIndex++ {
		peerIndex := peerIndex

		group.Go(func() error {
			peer := NewPeer(PeerConfig{
				RoomID:                m.room.RoomID,
				Password:              m.room.Password,
				ConnectorURL:          connectorURL,
				APIClient:             m.apiClient,
				ParticipantName:       peerName(m.role, m.sessionID, peerIndex),
				TargetParticipantName: peerName(oppositeRole(m.role), m.sessionID, peerIndex),
				Logger: m.log.With(
					slog.Int("peerIndex", peerIndex+1),
					slog.String("roomId", m.room.RoomID),
					slog.String("session", m.sessionID),
				),
			})

			if err := peer.Connect(groupCtx); err != nil {
				peer.Close()
				return fmt.Errorf("connect peer %d: %w", peerIndex+1, err)
			}

			// Each peer owns its own reconnect loop; the tunnel layer only sees
			// a flat slice of transports.
			go peer.WatchConnection(groupCtx)
			peers[peerIndex] = peer
			return nil
		})
	}

	if err := group.Wait(); err != nil {
		for _, connected := range peers {
			if connected != nil {
				connected.Close()
			}
		}
		return nil, err
	}

	return peers, nil
}

func peerName(role, sessionID string, peerIndex int) string {
	return fmt.Sprintf("jazztun-%s-%s-%d", role, sessionID, peerIndex+1)
}

func oppositeRole(role string) string {
	if role == "client" {
		return "server"
	}
	return "client"
}

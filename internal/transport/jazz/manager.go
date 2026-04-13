package jazz

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/Kavun-Sama/jazztun/internal/transport"
)

// Manager provisions and connects transport peers across one or more rooms.
type Manager struct {
	apiClient    *APIClient
	rooms        []RoomSpec
	peersPerRoom int
	role         string
	log          *slog.Logger
}

// ManagerConfig describes how to build a room/peer topology.
type ManagerConfig struct {
	APIClient    *APIClient
	Rooms        []RoomSpec
	PeersPerRoom int
	Role         string
	Logger       *slog.Logger
}

// NewManager creates a transport manager for one side of the tunnel.
func NewManager(cfg ManagerConfig) (*Manager, error) {
	if cfg.APIClient == nil {
		return nil, fmt.Errorf("api client is required")
	}
	if len(cfg.Rooms) == 0 {
		return nil, fmt.Errorf("at least one room is required")
	}
	if cfg.PeersPerRoom <= 0 {
		return nil, fmt.Errorf("peers per room must be > 0")
	}
	if cfg.Role != "client" && cfg.Role != "server" {
		return nil, fmt.Errorf("invalid role %q", cfg.Role)
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	return &Manager{
		apiClient:    cfg.APIClient,
		rooms:        cfg.Rooms,
		peersPerRoom: cfg.PeersPerRoom,
		role:         cfg.Role,
		log:          cfg.Logger.With(slog.String("component", "jazz/manager")),
	}, nil
}

// ConnectAll creates, connects, and starts reconnection loops for all peers.
func (m *Manager) ConnectAll(ctx context.Context) ([]transport.Transport, error) {
	peers := make([]transport.Transport, 0, len(m.rooms)*m.peersPerRoom)

	for roomIndex, room := range m.rooms {
		connectorURL := room.ConnectorURL
		if connectorURL == "" {
			preResp, err := m.apiClient.Preconnect(room.RoomID, room.Password)
			if err != nil {
				m.log.Warn("preconnect failed, using default connector",
					"error", err,
					"roomId", room.RoomID,
					"connectorUrl", DefaultConnectorURL,
				)
				connectorURL = DefaultConnectorURL
			} else {
				connectorURL = preResp.ConnectorURL
			}
		}

		for peerIndex := 0; peerIndex < m.peersPerRoom; peerIndex++ {
			peer := NewPeer(PeerConfig{
				RoomID:                room.RoomID,
				Password:              room.Password,
				ConnectorURL:          connectorURL,
				APIClient:             m.apiClient,
				ParticipantName:       peerName(m.role, roomIndex, peerIndex),
				TargetParticipantName: peerName(oppositeRole(m.role), roomIndex, peerIndex),
				Logger: m.log.With(
					slog.Int("roomIndex", roomIndex+1),
					slog.Int("peerIndex", peerIndex+1),
					slog.String("roomId", room.RoomID),
				),
			})

			if err := peer.Connect(ctx); err != nil {
				for _, connected := range peers {
					connected.Close()
				}
				return nil, fmt.Errorf("connect room %d peer %d: %w", roomIndex+1, peerIndex+1, err)
			}

			go peer.WatchConnection(ctx)
			peers = append(peers, peer)
		}
	}

	return peers, nil
}

func peerName(role string, roomIndex, peerIndex int) string {
	return fmt.Sprintf("jazztun-r%d-%s-%d", roomIndex+1, role, peerIndex+1)
}

func oppositeRole(role string) string {
	if role == "client" {
		return "server"
	}
	return "client"
}

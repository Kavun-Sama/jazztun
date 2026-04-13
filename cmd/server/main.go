package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/Kavun-Sama/salute-jazz-rtc-tunnel/internal/crypto"
	"github.com/Kavun-Sama/salute-jazz-rtc-tunnel/internal/transport"
	"github.com/Kavun-Sama/salute-jazz-rtc-tunnel/internal/transport/jazz"
	"github.com/Kavun-Sama/salute-jazz-rtc-tunnel/internal/tunnel"
)

func main() {
	room := flag.String("room", "", "Jazz room URL or 'new' to create a new one")
	key := flag.String("key", "", "hex 32 bytes encryption key (if empty, generate and print)")
	duo := flag.Bool("duo", false, "use 2 transport peers in parallel")
	peersFlag := flag.Int("peers", 0, "number of transport peers to open (overrides -duo)")
	dns := flag.String("dns", "1.1.1.1:53", "DNS server")
	socksProxy := flag.String("socks", "", "upstream SOCKS5 proxy addr:port")
	verbose := flag.Bool("v", false, "verbose logging")
	flag.Parse()

	// Setup logging
	logLevel := slog.LevelInfo
	if *verbose {
		logLevel = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel}))
	slog.SetDefault(logger)

	// Generate or decode key
	keyBytes, err := resolveKey(*key)
	if err != nil {
		logger.Error("key error", "error", err)
		os.Exit(1)
	}

	cipher, err := crypto.NewCipher(keyBytes)
	if err != nil {
		logger.Error("cipher error", "error", err)
		os.Exit(1)
	}

	// Setup API client
	api := jazz.NewAPIClient(logger)

	// Resolve room
	roomID, password, err := resolveRoom(api, *room, logger)
	if err != nil {
		logger.Error("room error", "error", err)
		os.Exit(1)
	}

	// Preconnect
	preResp, err := api.Preconnect(roomID, password)
	if err != nil {
		logger.Warn("preconnect failed, using default connector", "error", err, "connectorUrl", jazz.DefaultConnectorURL)
		preResp = &jazz.PreconnectResponse{
			ConnectorURL: jazz.DefaultConnectorURL,
		}
	}

	// Create transport peers
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	peerCount, err := resolvePeerCount(*peersFlag, *duo)
	if err != nil {
		logger.Error("peer config error", "error", err)
		os.Exit(1)
	}

	peers := make([]transport.Transport, 0, peerCount)
	for i := 0; i < peerCount; i++ {
		peer := jazz.NewPeer(jazz.PeerConfig{
			RoomID:                roomID,
			Password:              password,
			ConnectorURL:          preResp.ConnectorURL,
			APIClient:             api,
			ParticipantName:       peerName("server", i),
			TargetParticipantName: peerName("client", i),
			Logger:                logger,
		})

		if err := peer.Connect(ctx); err != nil {
			logger.Error("peer connect failed", "error", err, "peer", i)
			os.Exit(1)
		}

		go peer.WatchConnection(ctx)
		peers = append(peers, peer)
	}

	logger.Info("all peers connected",
		"roomId", roomID,
		"peers", peerCount,
		"key", hex.EncodeToString(keyBytes),
	)

	// Create and run tunnel server
	srv := tunnel.NewServer(tunnel.ServerConfig{
		Peers:  peers,
		Cipher: cipher,
		DNS:    *dns,
		Socks:  *socksProxy,
		Logger: logger,
	})

	// Handle signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		logger.Info("shutting down")
		cancel()
	}()

	if err := srv.Run(ctx); err != nil {
		logger.Error("server error", "error", err)
		os.Exit(1)
	}
}

func resolvePeerCount(peers int, duo bool) (int, error) {
	if peers < 0 {
		return 0, fmt.Errorf("peers must be >= 0")
	}
	if peers > 0 {
		return peers, nil
	}
	if duo {
		return 2, nil
	}
	return 1, nil
}

func peerName(role string, index int) string {
	return fmt.Sprintf("olcrtc-%s-%d", role, index+1)
}

func resolveKey(keyHex string) ([]byte, error) {
	if keyHex == "" {
		key := make([]byte, 32)
		if _, err := rand.Read(key); err != nil {
			return nil, fmt.Errorf("generate key: %w", err)
		}
		fmt.Fprintf(os.Stderr, "Generated key: %s\n", hex.EncodeToString(key))
		return key, nil
	}

	key, err := hex.DecodeString(keyHex)
	if err != nil {
		return nil, fmt.Errorf("decode hex key: %w", err)
	}
	if len(key) != 32 {
		return nil, fmt.Errorf("key must be 32 bytes, got %d", len(key))
	}
	return key, nil
}

func resolveRoom(api *jazz.APIClient, room string, logger *slog.Logger) (roomID, password string, err error) {
	if room == "" {
		return "", "", fmt.Errorf("room is required (-room flag)")
	}

	if room == "new" {
		resp, err := api.CreateRoom()
		if err != nil {
			return "", "", fmt.Errorf("create room: %w", err)
		}
		logger.Info("created room", "url", resp.URL, "roomId", resp.RoomID, "password", resp.Password)
		return resp.RoomID, resp.Password, nil
	}

	roomID, password, err = jazz.ParseRoomURL(room)
	if err != nil {
		return "", "", fmt.Errorf("parse room URL: %w", err)
	}

	if password == "" {
		return "", "", fmt.Errorf("room URL must include password (psw parameter)")
	}

	return roomID, password, nil
}

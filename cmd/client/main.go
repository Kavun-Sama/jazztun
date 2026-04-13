package main

import (
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Kavun-Sama/jazztun/internal/crypto"
	"github.com/Kavun-Sama/jazztun/internal/transport/jazz"
	"github.com/Kavun-Sama/jazztun/internal/tunnel"
)

func main() {
	room := flag.String("room", "", "Jazz room URL or comma-separated room URL list")
	key := flag.String("key", "", "hex 32 bytes encryption key")
	listen := flag.String("listen", "127.0.0.1:1080", "local SOCKS5 address")
	duo := flag.Bool("duo", false, "use 2 transport peers in parallel")
	peersFlag := flag.Int("peers", 0, "number of transport peers to open (overrides -duo)")
	roomsFlag := flag.Int("rooms", 0, "expected number of rooms in -room list (0 = infer from list)")
	verbose := flag.Bool("v", false, "verbose logging")
	flag.Parse()

	// Setup logging
	logLevel := slog.LevelInfo
	if *verbose {
		logLevel = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel}))
	slog.SetDefault(logger)

	if *room == "" {
		logger.Error("room is required (-room flag)")
		os.Exit(1)
	}
	if *key == "" {
		logger.Error("key is required (-key flag)")
		os.Exit(1)
	}

	// Decode key
	keyBytes, err := hex.DecodeString(*key)
	if err != nil {
		logger.Error("decode key", "error", err)
		os.Exit(1)
	}
	if len(keyBytes) != 32 {
		logger.Error("key must be 32 bytes", "got", len(keyBytes))
		os.Exit(1)
	}

	cipher, err := crypto.NewCipher(keyBytes)
	if err != nil {
		logger.Error("cipher error", "error", err)
		os.Exit(1)
	}

	roomSpecs, err := resolveRooms(*room, *roomsFlag)
	if err != nil {
		logger.Error("room config error", "error", err)
		os.Exit(1)
	}

	api := jazz.NewAPIClient(logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	peerCount, err := resolvePeerCount(*peersFlag, *duo)
	if err != nil {
		logger.Error("peer config error", "error", err)
		os.Exit(1)
	}

	manager, err := jazz.NewManager(jazz.ManagerConfig{
		APIClient:    api,
		Rooms:        roomSpecs,
		PeersPerRoom: peerCount,
		Role:         "client",
		Logger:       logger,
	})
	if err != nil {
		logger.Error("manager config error", "error", err)
		os.Exit(1)
	}

	peers, err := manager.ConnectAll(ctx)
	if err != nil {
		logger.Error("peer connect failed", "error", err)
		os.Exit(1)
	}

	logger.Info("all peers connected",
		"rooms", len(roomSpecs),
		"peersPerRoom", peerCount,
		"totalPeers", len(peers),
	)

	// Generate client ID
	clientID := generateClientID()

	// Create and run tunnel client
	client := tunnel.NewClient(tunnel.ClientConfig{
		Peers:    peers,
		Cipher:   cipher,
		Listen:   *listen,
		ClientID: clientID,
		Logger:   logger,
	})

	logger.Info("starting SOCKS5 proxy",
		"listen", *listen,
		"clientID", fmt.Sprintf("%08x", clientID),
	)

	// Handle signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		logger.Info("shutting down")
		cancel()
	}()

	if err := client.Run(ctx); err != nil {
		logger.Error("client error", "error", err)
		os.Exit(1)
	}
}

func generateClientID() uint32 {
	t := uint32(time.Now().Unix()) & 0xFFFF0000
	r := rand.Uint32() & 0x0000FFFF
	return t | r
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

func resolveRooms(roomArg string, expectedCount int) ([]jazz.RoomSpec, error) {
	if roomArg == "" {
		return nil, fmt.Errorf("room is required (-room flag)")
	}

	rooms, err := jazz.ParseRoomList(roomArg)
	if err != nil {
		return nil, err
	}

	if expectedCount > 0 && len(rooms) != expectedCount {
		return nil, fmt.Errorf("expected %d rooms, got %d", expectedCount, len(rooms))
	}

	return rooms, nil
}

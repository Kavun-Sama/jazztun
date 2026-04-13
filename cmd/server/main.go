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

	"github.com/Kavun-Sama/jazztun/internal/crypto"
	"github.com/Kavun-Sama/jazztun/internal/transport/jazz"
	"github.com/Kavun-Sama/jazztun/internal/tunnel"
)

func main() {
	room := flag.String("room", "", "Jazz room URL list or 'new' to create room(s)")
	key := flag.String("key", "", "hex 32 bytes encryption key (if empty, generate and print)")
	duo := flag.Bool("duo", false, "use 2 transport peers in parallel")
	peersFlag := flag.Int("peers", 0, "number of transport peers to open (overrides -duo)")
	roomsFlag := flag.Int("rooms", 0, "number of rooms to create with -room=new, or expected room count for a room list (0 = infer/default 1)")
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

	roomSpecs, err := resolveRooms(api, *room, *roomsFlag, logger)
	if err != nil {
		logger.Error("room config error", "error", err)
		os.Exit(1)
	}

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
		Role:         "server",
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
		"roomArg", jazz.JoinRoomURLs(roomSpecs),
		"peersPerRoom", peerCount,
		"totalPeers", len(peers),
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

func resolveRooms(api *jazz.APIClient, roomArg string, roomCount int, logger *slog.Logger) ([]jazz.RoomSpec, error) {
	if roomArg == "" {
		return nil, fmt.Errorf("room is required (-room flag)")
	}

	if roomArg == "new" {
		if roomCount <= 0 {
			roomCount = 1
		}
		rooms, err := api.CreateRooms(roomCount)
		if err != nil {
			return nil, fmt.Errorf("create rooms: %w", err)
		}
		for i, room := range rooms {
			logger.Info("created room",
				"index", i+1,
				"roomId", room.RoomID,
				"url", room.URL,
			)
		}
		logger.Info("created room set", "rooms", len(rooms), "roomArg", jazz.JoinRoomURLs(rooms))
		return rooms, nil
	}

	rooms, err := jazz.ParseRoomList(roomArg)
	if err != nil {
		return nil, err
	}
	if roomCount > 0 && len(rooms) != roomCount {
		return nil, fmt.Errorf("expected %d rooms, got %d", roomCount, len(rooms))
	}

	return rooms, nil
}

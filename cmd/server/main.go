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

	"github.com/Kavun-Sama/jazztun/internal/buildinfo"
	"github.com/Kavun-Sama/jazztun/internal/crypto"
	"github.com/Kavun-Sama/jazztun/internal/transport/jazz"
	"github.com/Kavun-Sama/jazztun/internal/tunnel"
)

func main() {
	room := flag.String("room", "", "Jazz room URL or 'new' to create a room")
	key := flag.String("key", "", "hex 32 bytes encryption key (if empty, generate and print)")
	duo := flag.Bool("duo", false, "use 2 transport peers in parallel")
	peersFlag := flag.Int("peers", 0, "number of transport peers to open (overrides -duo)")
	versionFlag := flag.Bool("version", false, "print version and exit")
	dns := flag.String("dns", "1.1.1.1:53", "DNS server")
	socksProxy := flag.String("socks", "", "upstream SOCKS5 proxy addr:port")
	verbose := flag.Bool("v", false, "verbose logging")
	flag.Parse()

	if *versionFlag {
		fmt.Println(buildinfo.Version)
		return
	}

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

	roomSpec, err := resolveRoom(api, *room, logger)
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
		Room:         roomSpec,
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
		"version", buildinfo.Version,
		"roomId", roomSpec.RoomID,
		"roomURL", roomSpec.URL,
		"peersPerRoom", peerCount,
		"totalPeers", len(peers),
	)
	logger.Info("jazztun server ready",
		"version", buildinfo.Version,
		"roomId", roomSpec.RoomID,
		"peersPerRoom", peerCount,
		"totalPeers", len(peers),
		"dns", *dns,
		"socks", *socksProxy,
		"keyPrefix", hex.EncodeToString(keyBytes[:4]),
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

func resolveRoom(api *jazz.APIClient, roomArg string, logger *slog.Logger) (jazz.RoomSpec, error) {
	if roomArg == "" {
		return jazz.RoomSpec{}, fmt.Errorf("room is required (-room flag)")
	}

	if roomArg == "new" {
		resp, err := api.CreateRoom()
		if err != nil {
			return jazz.RoomSpec{}, fmt.Errorf("create room: %w", err)
		}
		logger.Info("created room",
			"roomId", resp.RoomID,
			"url", resp.URL,
		)
		return jazz.RoomSpec{
			RoomID:   resp.RoomID,
			Password: resp.Password,
			URL:      resp.URL,
		}, nil
	}

	roomID, password, err := jazz.ParseRoomURL(roomArg)
	if err != nil {
		return jazz.RoomSpec{}, fmt.Errorf("parse room URL: %w", err)
	}
	if password == "" {
		return jazz.RoomSpec{}, fmt.Errorf("room URL must include password (psw parameter)")
	}

	return jazz.RoomSpec{
		RoomID:   roomID,
		Password: password,
		URL:      roomArg,
	}, nil
}

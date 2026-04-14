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

	"github.com/Kavun-Sama/jazztun/internal/buildinfo"
	"github.com/Kavun-Sama/jazztun/internal/crypto"
	"github.com/Kavun-Sama/jazztun/internal/socks"
	"github.com/Kavun-Sama/jazztun/internal/transport/jazz"
	"github.com/Kavun-Sama/jazztun/internal/tunnel"
)

func main() {
	room := flag.String("room", "", "Jazz room URL")
	key := flag.String("key", "", "hex 32 bytes encryption key")
	listen := flag.String("listen", "127.0.0.1:1080", "local SOCKS5 address")
	socksUser := flag.String("socks-user", "", "SOCKS5 username for local proxy auth")
	socksPass := flag.String("socks-pass", "", "SOCKS5 password for local proxy auth")
	duo := flag.Bool("duo", false, "use 2 transport peers in parallel")
	peersFlag := flag.Int("peers", 0, "number of transport peers to open (overrides -duo)")
	versionFlag := flag.Bool("version", false, "print version and exit")
	verbose := flag.Bool("v", false, "verbose logging")
	flag.Parse()

	if *versionFlag {
		fmt.Println(buildinfo.Version)
		return
	}

	// Setup logging
	logLevel := slog.LevelError
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
	if (*socksUser == "") != (*socksPass == "") {
		logger.Error("both -socks-user and -socks-pass must be set together")
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

	roomSpec, err := resolveRoom(*room)
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
		Room:         roomSpec,
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

	// Generate client ID
	clientID := generateClientID()

	// Create and run tunnel client
	client := tunnel.NewClient(tunnel.ClientConfig{
		Peers:    peers,
		Cipher:   cipher,
		Listen:   *listen,
		ClientID: clientID,
		SOCKSAuth: socks.AuthConfig{
			Username: *socksUser,
			Password: *socksPass,
		},
		Logger: logger,
	})
	printClientStartup(*listen, roomSpec.RoomID, peerCount, *socksUser != "", clientID)

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

func resolveRoom(roomArg string) (jazz.RoomSpec, error) {
	if roomArg == "" {
		return jazz.RoomSpec{}, fmt.Errorf("room is required (-room flag)")
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

func printClientStartup(listen, roomID string, peers int, socksAuth bool, clientID uint32) {
	fmt.Printf("jazztun %s client ready\n", buildinfo.Version)
	fmt.Printf("  listen:     %s\n", listen)
	fmt.Printf("  room id:    %s\n", roomID)
	fmt.Printf("  peers:      %d\n", peers)
	fmt.Printf("  socks auth: %t\n", socksAuth)
	fmt.Printf("  client id:  %08x\n", clientID)
	fmt.Println()
	fmt.Println("Configure your app to use SOCKS5 at the address above.")
}

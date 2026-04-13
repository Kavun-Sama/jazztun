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

	"github.com/Kavun-Sama/salute-jazz-rtc-tunnel/internal/crypto"
	"github.com/Kavun-Sama/salute-jazz-rtc-tunnel/internal/transport"
	"github.com/Kavun-Sama/salute-jazz-rtc-tunnel/internal/transport/jazz"
	"github.com/Kavun-Sama/salute-jazz-rtc-tunnel/internal/tunnel"
)

func main() {
	room := flag.String("room", "", "Jazz room URL")
	key := flag.String("key", "", "hex 32 bytes encryption key")
	listen := flag.String("listen", "127.0.0.1:1080", "local SOCKS5 address")
	duo := flag.Bool("duo", false, "use 2 transport peers in parallel")
	peersFlag := flag.Int("peers", 0, "number of transport peers to open (overrides -duo)")
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

	// Parse room URL
	roomID, password, err := jazz.ParseRoomURL(*room)
	if err != nil {
		logger.Error("parse room URL", "error", err)
		os.Exit(1)
	}
	if password == "" {
		logger.Error("room URL must include password (psw parameter)")
		os.Exit(1)
	}

	// Setup API client and preconnect
	api := jazz.NewAPIClient(logger)

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
			ParticipantName:       peerName("client", i),
			TargetParticipantName: peerName("server", i),
			Logger:                logger,
		})

		if err := peer.Connect(ctx); err != nil {
			logger.Error("peer connect failed", "error", err, "peer", i)
			os.Exit(1)
		}

		go peer.WatchConnection(ctx)
		peers = append(peers, peer)
	}

	logger.Info("all peers connected", "roomId", roomID, "peers", peerCount)

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

func peerName(role string, index int) string {
	return fmt.Sprintf("olcrtc-%s-%d", role, index+1)
}

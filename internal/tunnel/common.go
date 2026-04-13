package tunnel

import (
	"fmt"
	"time"

	icrypto "github.com/Kavun-Sama/salute-jazz-rtc-tunnel/internal/crypto"
	"github.com/Kavun-Sama/salute-jazz-rtc-tunnel/internal/transport"
)

const cmdConnect = "connect"

func streamPeerIndex(peerCount int, clientID uint32, sid uint16) int {
	if peerCount <= 1 {
		return 0
	}
	hash := clientID*2654435761 ^ uint32(sid)
	return int(hash % uint32(peerCount))
}

func sendEncryptedFrame(peer transport.Transport, frameCipher *icrypto.Cipher, data []byte) error {
	encrypted, err := frameCipher.Encrypt(data)
	if err != nil {
		return fmt.Errorf("encrypt: %w", err)
	}

	backoff := 250 * time.Microsecond
	for {
		if peer.CanSend() {
			return peer.Send(encrypted)
		}

		timer := time.NewTimer(backoff)
		select {
		case <-peer.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return fmt.Errorf("transport disconnected before send")
		case <-timer.C:
		}

		if backoff < 10*time.Millisecond {
			backoff *= 2
		}
	}
}

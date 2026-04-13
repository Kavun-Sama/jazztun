package jazz

import (
	"fmt"

	livekit "github.com/livekit/protocol/livekit"
	"google.golang.org/protobuf/proto"
)

// EncodeDataPacket wraps payload into an official LiveKit DataPacket protobuf.
func EncodeDataPacket(payload []byte, destinationIdentities []string) ([]byte, error) {
	packet := &livekit.DataPacket{
		Kind:                  livekit.DataPacket_RELIABLE,
		DestinationIdentities: destinationIdentities,
		Value: &livekit.DataPacket_User{
			User: &livekit.UserPacket{
				Payload:               payload,
				DestinationIdentities: destinationIdentities,
			},
		},
	}

	data, err := proto.Marshal(packet)
	if err != nil {
		return nil, fmt.Errorf("marshal data packet: %w", err)
	}
	return data, nil
}

// DecodeDataPacket extracts the user payload from an official LiveKit DataPacket protobuf.
func DecodeDataPacket(data []byte) ([]byte, error) {
	var packet livekit.DataPacket
	if err := proto.Unmarshal(data, &packet); err != nil {
		return nil, fmt.Errorf("unmarshal data packet: %w", err)
	}

	user := packet.GetUser()
	if user == nil {
		return nil, fmt.Errorf("no user packet in data packet")
	}

	return user.GetPayload(), nil
}

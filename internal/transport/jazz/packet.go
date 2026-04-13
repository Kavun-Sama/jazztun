package jazz

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// LiveKit DataPacket manual protobuf encoding/decoding.
// We only handle the UserPacket variant with payload field.
//
// DataPacket wire format:
//   Field 1 (kind): varint, tag=0x08, value=0 (RELIABLE)
//   Field 2 (user): length-delimited, tag=0x12
//     UserPacket:
//       Field 2 (payload): length-delimited, tag=0x12, value=our data

// EncodeDataPacket wraps payload into a LiveKit DataPacket protobuf message.
func EncodeDataPacket(payload []byte) []byte {
	// Encode inner UserPacket: field 2 (payload)
	userPayload := encodeLengthDelimited(0x12, payload)

	// Encode outer DataPacket: field 1 (kind=RELIABLE=0) + field 2 (user)
	// Field 1: tag=0x08, value=0 (varint) - omit since default value is 0
	// Actually, we should include it for clarity
	outer := make([]byte, 0, 2+len(userPayload)+10)
	// kind = RELIABLE = 0
	outer = append(outer, 0x08, 0x00)
	// user packet as field 2
	outer = append(outer, encodeLengthDelimited(0x12, userPayload)...)

	return outer
}

// DecodeDataPacket extracts the payload from a LiveKit DataPacket protobuf message.
func DecodeDataPacket(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, errors.New("empty data packet")
	}

	// Parse outer DataPacket, look for field 2 (user packet)
	userPacket, err := findField(data, 2)
	if err != nil {
		return nil, fmt.Errorf("find user packet field: %w", err)
	}
	if userPacket == nil {
		return nil, errors.New("no user packet in data packet")
	}

	// Parse UserPacket, look for field 2 (payload)
	payload, err := findField(userPacket, 2)
	if err != nil {
		return nil, fmt.Errorf("find payload field: %w", err)
	}
	if payload == nil {
		return nil, errors.New("no payload in user packet")
	}

	return payload, nil
}

// encodeLengthDelimited encodes a protobuf length-delimited field.
// tag is the full tag byte (field_number << 3 | 2).
func encodeLengthDelimited(tag byte, data []byte) []byte {
	lenBytes := encodeVarint(uint64(len(data)))
	out := make([]byte, 0, 1+len(lenBytes)+len(data))
	out = append(out, tag)
	out = append(out, lenBytes...)
	out = append(out, data...)
	return out
}

// encodeVarint encodes a uint64 as a protobuf varint.
func encodeVarint(v uint64) []byte {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], v)
	return buf[:n]
}

// findField searches protobuf wire-format data for a field with the given number.
// Returns the field value (for length-delimited fields) or nil if not found.
func findField(data []byte, fieldNum uint64) ([]byte, error) {
	pos := 0
	for pos < len(data) {
		// Read tag (varint)
		tag, n := binary.Uvarint(data[pos:])
		if n <= 0 {
			return nil, errors.New("invalid varint for tag")
		}
		pos += n

		wireType := tag & 0x07
		field := tag >> 3

		switch wireType {
		case 0: // varint
			_, n = binary.Uvarint(data[pos:])
			if n <= 0 {
				return nil, errors.New("invalid varint value")
			}
			pos += n

		case 1: // 64-bit fixed
			if pos+8 > len(data) {
				return nil, errors.New("truncated 64-bit field")
			}
			pos += 8

		case 2: // length-delimited
			length, n := binary.Uvarint(data[pos:])
			if n <= 0 {
				return nil, errors.New("invalid varint for length")
			}
			pos += n

			if pos+int(length) > len(data) {
				return nil, fmt.Errorf("truncated length-delimited field: need %d, have %d", length, len(data)-pos)
			}

			if field == fieldNum {
				return data[pos : pos+int(length)], nil
			}
			pos += int(length)

		case 5: // 32-bit fixed
			if pos+4 > len(data) {
				return nil, errors.New("truncated 32-bit field")
			}
			pos += 4

		default:
			return nil, fmt.Errorf("unknown wire type %d", wireType)
		}
	}

	return nil, nil
}

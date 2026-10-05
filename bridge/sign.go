package bridge

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"strings"
)

const (
	protocolVersion = 2
	commandType     = 7
	deviceID        = 1
)

// signCommand builds the /to_lock payload (all integers big-endian):
// message_id u64=0 | protocol u8 | command_type u8 | timestamp u64 |
// HMAC-SHA256(secret, protocol|command_type|timestamp|key_id|device_id|action) |
// key_id u8 | device_id u8 | action u8
func signCommand(secret []byte, localID uint8, a Action, ts int64) []byte {
	signed := []byte{protocolVersion, commandType}
	signed = binary.BigEndian.AppendUint64(signed, uint64(ts))
	signed = append(signed, localID, deviceID, byte(a))
	mac := hmac.New(sha256.New, secret)
	mac.Write(signed)

	out := make([]byte, 8, 8+2+8+sha256.Size+3) // message_id = 0
	out = append(out, protocolVersion, commandType)
	out = binary.BigEndian.AppendUint64(out, uint64(ts))
	out = mac.Sum(out)
	return append(out, localID, deviceID, byte(a))
}

// encodeCommand matches Python's urllib.parse.quote(base64): '+' and '='
// are escaped, '/' stays literal.
var commandEscaper = strings.NewReplacer("+", "%2B", "=", "%3D")

func encodeCommand(b []byte) string {
	return commandEscaper.Replace(base64.StdEncoding.EncodeToString(b))
}

// hashHex returns hex(sha256(parts...)).
func hashHex(parts ...[]byte) string {
	h := sha256.New()
	for _, p := range parts {
		h.Write(p)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func be64(v uint64) []byte { return binary.BigEndian.AppendUint64(nil, v) }
func be32(v uint32) []byte { return binary.BigEndian.AppendUint32(nil, v) }

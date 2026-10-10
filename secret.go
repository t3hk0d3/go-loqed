package loqed

import (
	"fmt"
	"log/slog"
	"strconv"
)

// Redacted is what a non-empty Secret prints as.
const Redacted = "[redacted]"

// Secret is a credential (lock key, token, password) that hides itself
// from fmt, log/slog and encoding/json, also when it is an exported field
// of a printed struct or an element of a printed slice. fmt prints
// unexported fields without calling their methods, so a Secret in an
// unexported field is not redacted. Reveal (or a string conversion)
// returns the value for signing, requests or storage.
//
// Decoding JSON, YAML or text into a Secret keeps the real value; encoding
// it to JSON yields Redacted, so a Secret is never persisted by accident.
// An empty Secret prints as empty, so "not set" stays visible.
type Secret string

// Reveal returns the raw value.
func (s Secret) Reveal() string { return string(s) }

func (s Secret) String() string {
	if s == "" {
		return ""
	}
	return Redacted
}

func (s Secret) GoString() string { return strconv.Quote(s.String()) }

// Format redacts every verb (%v, %s, %q, %x, ...).
func (s Secret) Format(f fmt.State, verb rune) {
	if verb == 'v' && f.Flag('#') {
		_, _ = f.Write([]byte(s.GoString()))
		return
	}
	switch verb {
	case 'v', 's', 'q':
		_, _ = fmt.Fprintf(f, fmt.FormatString(f, verb), s.String())
	default: // %x and friends would hex-encode the marker
		_, _ = f.Write([]byte(s.String()))
	}
}

func (s Secret) LogValue() slog.Value { return slog.StringValue(s.String()) }

func (s Secret) MarshalJSON() ([]byte, error) { return []byte(strconv.Quote(s.String())), nil }

package loqed

import (
	"strings"
)

// BoltState is the physical position of the lock bolt.
type BoltState string

// Bolt states. BoltUnknown covers anything LOQED reports that is not one of
// the three positions, including a lock that is offline or jammed.
const (
	BoltUnknown   BoltState = "unknown"
	BoltOpen      BoltState = "open"
	BoltDayLock   BoltState = "day_lock"
	BoltNightLock BoltState = "night_lock"
)

// ParseBoltState normalizes the spellings LOQED uses across APIs
// ("NIGHT_LOCK", "night_lock", "LATCH", "DAY_LOCK", "OPEN", ...).
// Anything unrecognized is BoltUnknown.
func ParseBoltState(s string) BoltState {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "open":
		return BoltOpen
	case "day_lock", "latch":
		return BoltDayLock
	case "night_lock":
		return BoltNightLock
	default:
		return BoltUnknown
	}
}

// ReachedState derives the bolt state a "state reached" webhook reports
// from its event_type, exactly like loqedAPI (requested_state is only what
// was asked for and must not be trusted). MOTOR_STALL reports jammed with an
// unknown bolt position.
func ReachedState(eventType string) (state BoltState, jammed bool) {
	et := strings.TrimSuffix(strings.ToUpper(strings.TrimSpace(eventType)), "_REMOTE")
	switch et {
	case "STATE_CHANGED_OPEN":
		return BoltOpen, false
	case "STATE_CHANGED_LATCH":
		return BoltDayLock, false
	case "STATE_CHANGED_NIGHT_LOCK":
		return BoltNightLock, false
	case "MOTOR_STALL":
		return BoltUnknown, true
	default:
		return BoltUnknown, false
	}
}

// IsGoToState reports whether eventType announces bolt movement
// (GO_TO_STATE_*).
func IsGoToState(eventType string) bool {
	return strings.HasPrefix(strings.ToUpper(strings.TrimSpace(eventType)), "GO_TO_STATE_")
}

// GoToTarget returns the movement target: goToState when it is known,
// otherwise the suffix of the GO_TO_STATE_* event type.
func GoToTarget(eventType, goToState string) BoltState {
	if s := ParseBoltState(goToState); s != BoltUnknown {
		return s
	}
	et := strings.ToUpper(strings.TrimSpace(eventType))
	switch {
	case strings.HasSuffix(et, "_OPEN"), strings.HasSuffix(et, "_VIA_OUTSIDE_MODULE_PIN"):
		// A PIN on the outside keypad opens the door (observed 2026-10-06:
		// the bridge reports go_to_state OPEN, the cloud copy reports none).
		return BoltOpen
	case strings.HasSuffix(et, "_LATCH"), strings.HasSuffix(et, "_DAY_LOCK"):
		return BoltDayLock
	case strings.HasSuffix(et, "_NIGHT_LOCK"), strings.HasSuffix(et, "_TO_LOCK"):
		return BoltNightLock
	default:
		return BoltUnknown
	}
}

// UnmarshalJSON accepts any spelling understood by ParseBoltState.
func (b *BoltState) UnmarshalJSON(data []byte) error {
	var s String
	if err := s.UnmarshalJSON(data); err != nil {
		return err
	}
	*b = ParseBoltState(string(s))
	return nil
}

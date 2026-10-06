package gateway

import (
	"strings"
	"time"
)

// lockEvent is the last lock event received from either feed. LOQED may
// deliver a cloud event twice, and delivers most events through both the
// bridge and the cloud; repeats of the latest event are dropped.
type lockEvent struct {
	eventType string // upper case
	key       *int
	at        time.Time
}

// isDuplicate reports whether an event repeats the latest received lock
// event (same event type and key, from either feed) within
// DuplicateWindow. Otherwise the event becomes the latest one. Only the
// latest event is compared, by design (no list of recent events).
func (s *Supervisor) isDuplicate(eventType string, key *int, now time.Time) bool {
	if s.t.DuplicateWindow <= 0 {
		return false // deduplication disabled (event_dedup_enabled: false)
	}
	et := strings.ToUpper(eventType)
	if l := s.lastLockEvent; l != nil && l.eventType == et && sameKey(l.key, key) && now.Sub(l.at) <= s.t.DuplicateWindow {
		return true
	}
	s.lastLockEvent = &lockEvent{eventType: et, key: key, at: now}
	return false
}

// nameFromDuplicate lets a dropped cloud copy name the event its bridge copy
// already published (only the cloud carries key_name_user).
func (s *Supervisor) nameFromDuplicate(keyNameUser string) {
	if keyNameUser == "" || s.state.LastKeyName != nil {
		return
	}
	name := keyNameUser
	s.state.LastKeyName = &name
	s.publish()
}

// gatewayActive: a gateway command is in flight or was sent within
// GatewayWindow, so events with the gateway's key are its doing.
func (s *Supervisor) gatewayActive(now time.Time) bool {
	return !s.lastCommandSentAt.IsZero() && now.Sub(s.lastCommandSentAt) < s.t.GatewayWindow
}

func (s *Supervisor) isGatewayKey(key *int) bool {
	id := s.Record().LocalID
	return key != nil && id != nil && *key == *id
}

func sameKey(a, b *int) bool {
	return (a == nil && b == nil) || (a != nil && b != nil && *a == *b)
}

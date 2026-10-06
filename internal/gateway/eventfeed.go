package gateway

import (
	"slices"
	"strings"
	"time"
)

// lockEvent is a published lock event. LOQED delivers most events through
// both the bridge and the cloud (in either order, sometimes interleaved),
// and may deliver one again a few seconds later.
type lockEvent struct {
	feed      string
	eventType string // upper case
	key       *int
	at        time.Time
	paired    bool // its copy from the other feed already arrived
}

func (e *lockEvent) matches(eventType string, key *int) bool {
	return e.eventType == eventType && sameKey(e.key, key)
}

// isDuplicate reports whether an event is a copy of one already published
// within DuplicateWindow, and otherwise records it as published:
//   - it repeats the latest published event (any feed): a cloud
//     re-delivery, the other feed's copy, or the same action repeated
//     (jiggling the knob), which is noise;
//   - or it is the other feed's copy of a recent event whose copy has not
//     arrived yet (the feeds can interleave: cloud GO_TO, cloud
//     STATE_CHANGED, bridge GO_TO, bridge STATE_CHANGED).
//
// A real change back (day, night, day) is never dropped: the second "day"
// does not repeat the latest event, and its first copy pairs with nothing.
func (s *Supervisor) isDuplicate(feed, eventType string, key *int, now time.Time) bool {
	dup := s.copyOfPublished(feed, strings.ToUpper(eventType), key, now)
	s.log.Debug("lock event received", "feed", feed, "event_type", eventType, "key_local_id", keyArg(key), "duplicate", dup)
	return dup
}

func (s *Supervisor) copyOfPublished(feed, eventType string, key *int, now time.Time) bool {
	if s.t.DuplicateWindow <= 0 {
		return false // deduplication disabled (event_dedup_enabled: false)
	}
	s.published = slices.DeleteFunc(s.published, func(e *lockEvent) bool { return now.Sub(e.at) > s.t.DuplicateWindow })
	if n := len(s.published); n > 0 && s.published[n-1].matches(eventType, key) {
		last := s.published[n-1]
		if last.feed != feed {
			last.paired = true
		}
		return true
	}
	for _, e := range s.published {
		if e.feed != feed && !e.paired && e.matches(eventType, key) {
			e.paired = true
			return true
		}
	}
	s.published = append(s.published, &lockEvent{feed: feed, eventType: eventType, key: key, at: now})
	return false
}

func keyArg(key *int) any {
	if key == nil {
		return nil
	}
	return *key
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

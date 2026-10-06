package gateway

import (
	"slices"
	"strings"
	"time"

	"github.com/t3hk0d3/go-loqed/cloud"
)

// feed is where a lock event came from. LOQED may deliver each cloud event
// twice, and usually delivers the cloud copy before the bridge copy.
type feed int

const (
	feedBridge feed = iota
	feedCloud
)

type feedEvent struct {
	eventType string // upper case
	key       *int
	at        time.Time
}

func (e feedEvent) matches(eventType string, key *int) bool {
	return e.eventType == strings.ToUpper(eventType) && sameKey(e.key, key)
}

// heldCloud is a cloud copy waiting for its bridge copy (local mode).
type heldCloud struct {
	ev cloud.WebhookEvent
	at time.Time
}

// isDuplicate reports a repeat delivery of the same event from the same
// feed within DuplicateWindow, and otherwise remembers the event.
func (s *Supervisor) isDuplicate(f feed, eventType string, key *int, now time.Time) bool {
	seen := slices.DeleteFunc(s.seen[f], func(e feedEvent) bool { return now.Sub(e.at) > s.t.DuplicateWindow })
	for _, e := range seen {
		if e.matches(eventType, key) {
			s.seen[f] = seen
			return true
		}
	}
	s.seen[f] = append(seen, feedEvent{eventType: strings.ToUpper(eventType), key: key, at: now})
	return false
}

// holdCloud keeps a cloud copy until its bridge copy arrives.
func (s *Supervisor) holdCloud(now time.Time, e cloud.WebhookEvent) {
	s.pruneHeld(now)
	s.held = append(s.held, heldCloud{ev: e, at: now})
}

// takeHeld removes and returns the key name of a held cloud copy of a
// bridge event, if one arrived within EnrichWindow.
func (s *Supervisor) takeHeld(now time.Time, eventType string, key *int) string {
	s.pruneHeld(now)
	for i, h := range s.held {
		if strings.EqualFold(h.ev.EventType, eventType) && sameKey(h.ev.KeyLocalID, key) {
			s.held = slices.Delete(s.held, i, i+1)
			return h.ev.KeyNameUser
		}
	}
	return ""
}

// pruneHeld drops cloud copies whose bridge copy never came.
func (s *Supervisor) pruneHeld(now time.Time) {
	s.held = slices.DeleteFunc(s.held, func(h heldCloud) bool { return now.Sub(h.at) > s.t.EnrichWindow })
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

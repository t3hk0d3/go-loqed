package gateway

import (
	"context"
	"strings"
	"time"

	"github.com/t3hk0d3/go-loqed/cloud"
	"github.com/t3hk0d3/go-loqed/internal/model"
)

// onCloudEvent handles a cloud webhook. While the bridge webhook is
// registered in local mode it only adds a key name to a matching recent
// bridge event; otherwise it drives state.
func (s *Supervisor) onCloudEvent(ctx context.Context, e cloud.WebhookEvent) {
	now := s.d.Now()
	s.lastCloudEventAt = now
	lockEvent := e.Kind == cloud.KindStateReached || e.Kind == cloud.KindGoToState
	if lockEvent && s.isDuplicate(feedCloud, e.EventType, e.KeyLocalID, now) {
		return
	}
	if s.mode == model.ModeLocal && s.webhookOK {
		if lockEvent && !s.enrichFromCloud(now, e) {
			s.holdCloud(now, e)
		}
		return
	}
	if s.mode == model.ModeOffline {
		s.setMode(model.ModeCloud) // a cloud webhook proves the cloud works
		s.bridgeProbeSchedule(now)
		s.nextCloudProbe = now.Add(s.t.Liveness)
	}
	if s.mode == model.ModeCloud {
		// Push works: drop to the reconcile cadence, counted from the last
		// successful poll (so events never postpone it).
		// An event may pull the poll out to the reconcile cadence, never
		// push a due poll further away.
		if !s.lastPollAt.IsZero() {
			if r := s.lastPollAt.Add(s.t.Reconcile); r.After(now) && r.After(s.nextCloudPoll) {
				s.nextCloudPoll = r
			}
		}
	}
	switch e.Kind {
	case cloud.KindStateReached:
		s.state.LockOnline = true
		if !e.Jammed {
			s.move = movement{}
		}
		s.onReached(now, e.BoltState, e.Jammed)
		s.recordEvent(now, e.EventType, e.KeyLocalID, e.KeyNameUser, model.FromStateReached(e.EventType), feedCloud)
		s.cmds.onReached(ctx, now, e.BoltState, e.Jammed, e.KeyLocalID)
	case cloud.KindGoToState:
		s.startMovement(now, e.GoToState)
		s.recordEvent(now, e.EventType, e.KeyLocalID, e.KeyNameUser, model.FromGoTo(e.GoToState, s.state.Lock), feedCloud)
		s.cmds.onGoTo(now, e.GoToState, e.KeyLocalID)
	case cloud.KindSignal:
		if e.BatteryPercentage != nil && *e.BatteryPercentage >= 0 {
			s.state.BatteryPercentage = e.BatteryPercentage
		}
		if e.WifiStrength != nil {
			s.state.WifiStrength = e.WifiStrength
		}
		if e.BLEStrength != nil {
			s.state.BLEStrength = e.BLEStrength
		}
		// Any report from the lock itself means it is online again
		// (online: 1 is not always sent after a recovery).
		switch {
		case e.BLEStrength != nil:
			s.state.LockOnline = *e.BLEStrength != -1
		case e.BatteryPercentage != nil:
			s.state.LockOnline = *e.BatteryPercentage != -1
		default:
			s.state.LockOnline = true
		}
		s.publish()
	case cloud.KindOnline:
		if e.Online != nil {
			s.state.LockOnline = *e.Online
		}
		s.publish()
	}
}

// enrichFromCloud adds key_name_user to the bridge event it describes:
// same event type and key id, within EnrichWindow. It never emits an event.
// It reports whether the cloud copy matched a bridge event.
func (s *Supervisor) enrichFromCloud(now time.Time, e cloud.WebhookEvent) bool {
	last := s.lastBridgeEvent
	if last == nil || !strings.EqualFold(last.eventType, e.EventType) || now.Sub(last.at) > s.t.EnrichWindow ||
		!sameKey(last.keyID, e.KeyLocalID) {
		return false
	}
	if e.KeyNameUser != "" && s.state.LastKeyName == nil {
		name := e.KeyNameUser
		s.state.LastKeyName = &name
		s.publish()
	}
	return true
}

func sameKey(a, b *int) bool {
	return (a == nil && b == nil) || (a != nil && b != nil && *a == *b)
}

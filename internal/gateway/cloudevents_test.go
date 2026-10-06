package gateway

import (
	"errors"
	"testing"
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/cloud"
	"github.com/t3hk0d3/go-loqed/internal/config"
	"github.com/t3hk0d3/go-loqed/internal/model"
)

func cloudReached(name string) CloudEventMsg {
	return CloudEventMsg{Event: cloud.WebhookEvent{Kind: cloud.KindStateReached, LockID: "lock1", EventType: "STATE_CHANGED_NIGHT_LOCK",
		BoltState: loqed.BoltNightLock, RequestedState: loqed.BoltNightLock, KeyLocalID: model.Ptr(3), KeyNameUser: name}}
}

func TestCloudEventsDriveStateInCloudMode(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{}, func(d *Deps) { d.CloudWebhooks = true })
	h.start()
	h.toCloud()
	h.cloud.locks[0].BoltState = loqed.BoltDayLock
	h.advance(time.Minute)
	h.send(cloudReached("Front door key"))
	if h.lock() != "LOCKED" {
		t.Fatalf("lock %s", h.lock())
	}
	ev := h.pub.events[len(h.pub.events)-1]
	if ev.EventType != model.EventLocked || ev.KeyName == nil || *ev.KeyName != "Front door key" || *ev.KeyLocalID != 3 {
		t.Fatalf("event %+v", ev)
	}
	calls := len(h.cloud.calls)
	for range 60 {
		h.advance(time.Minute)
	}
	if len(h.cloud.calls) != calls {
		t.Fatal("with cloud webhooks working, background polling must stop")
	}
}

// Cloud events must not postpone the reconcile poll forever.
func TestReconcilePollIsScheduledFromLastPoll(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{}, func(d *Deps) { d.CloudWebhooks = true })
	h.start()
	h.toCloud()
	h.send(cloudReached(""))
	polls := len(h.cloud.calls)
	for range 25 { // an event every hour for a day
		h.advance(time.Hour)
		h.send(cloudReached(""))
	}
	if len(h.cloud.calls) <= polls {
		t.Fatal("the daily reconcile poll never ran")
	}
}

func TestCloudEventBringsOfflineLockBackToCloud(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.toCloud()
	h.cloudProbeErr = loqed.ErrUnreachable
	for range 3 {
		h.advance(time.Minute)
	}
	h.send(cloudReached(""))
	if h.s.mode != model.ModeCloud || !h.available() {
		t.Fatalf("mode %s", h.s.mode)
	}
}

func TestCloudEventEnrichesMatchingBridgeEvent(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.send(reached("STATE_CHANGED_NIGHT_LOCK", model.Ptr(3)))
	if h.state().LastKeyName != nil {
		t.Fatal("no name yet")
	}
	h.now = h.now.Add(5 * time.Second)
	h.send(cloudReached("Hallway phone"))
	if n := h.state().LastKeyName; n == nil || *n != "Hallway phone" {
		t.Fatalf("state %+v", h.state())
	}
	if len(h.pub.events) != 1 {
		t.Fatalf("enrichment must not emit a second event: %d", len(h.pub.events))
	}
}

func TestCloudEventWithOtherKeyIsANewEvent(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.send(reached("STATE_CHANGED_NIGHT_LOCK", model.Ptr(5)))
	h.send(cloudReached("Someone else")) // key 3
	if len(h.pub.events) != 2 {
		t.Fatalf("events %+v", h.pub.events)
	}
}

func TestCloudEventsDriveStateInLocalMode(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.send(cloudReached("x")) // no bridge copy (yet)
	if h.lock() != "LOCKED" || len(h.pub.events) != 1 {
		t.Fatalf("lock %s events %+v", h.lock(), h.pub.events)
	}
	h.advance(11 * time.Second)
	h.send(cloudReached("late"))
	if len(h.pub.events) != 2 {
		t.Fatal("a repeat outside the 10 s window is a new event")
	}
}

// Without a registered bridge webhook, cloud events are the only push
// source in local mode.
func TestCloudEventsDriveStateWhileBridgeWebhookIsMissing(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.bridge.listErr = errors.New("no free webhook slots")
	h.start()
	h.send(cloudReached(""))
	if h.s.mode != model.ModeLocal || h.lock() != "LOCKED" {
		t.Fatalf("mode %s lock %s", h.s.mode, h.lock())
	}
}

func TestConfiguredKeyNameBeatsCloudName(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{KeyNames: config.KeyNames{3: "Alice"}})
	h.start()
	h.toCloud()
	h.send(cloudReached("Front door key"))
	if n := h.state().LastKeyName; n == nil || *n != "Alice" {
		t.Fatalf("state %+v", h.state())
	}
}

// Cloud events arriving more often than the poll must not push a due
// reconcile poll further out.
func TestCloudEventsNeverPostponeDuePoll(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{}, func(d *Deps) { d.CloudWebhooks = true })
	h.start()
	h.toCloud()
	h.send(cloudReached(""))
	h.advance(h.s.lastPollAt.Add(h.s.t.Reconcile).Add(-time.Minute).Sub(h.now))
	polls := len(h.cloud.calls)
	for range 4 { // every 30 s for 2 min
		h.send(cloudReached(""))
		h.advance(30 * time.Second)
	}
	if len(h.cloud.calls) <= polls {
		t.Fatal("the due reconcile poll was postponed by cloud events")
	}
}

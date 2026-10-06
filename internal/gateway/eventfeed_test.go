package gateway

import (
	"testing"
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/bridge"
	"github.com/t3hk0d3/go-loqed/cloud"
	"github.com/t3hk0d3/go-loqed/internal/config"
	"github.com/t3hk0d3/go-loqed/internal/model"
)

func cloudGoTo(name string, key *int) CloudEventMsg {
	return CloudEventMsg{Event: cloud.WebhookEvent{Kind: cloud.KindGoToState, LockID: "6148", EventType: "GO_TO_STATE_MANUAL_LOCK_REMOTE_NIGHT_LOCK",
		GoToState: loqed.BoltNightLock, KeyLocalID: key, KeyNameUser: name}}
}

func bridgeGoTo(key *int) BridgeEventMsg {
	return BridgeEventMsg{Event: bridge.GoToStateEvent{EventType: "GO_TO_STATE_MANUAL_LOCK_REMOTE_NIGHT_LOCK",
		GoToState: loqed.BoltNightLock, KeyLocalID: key}}
}

func TestFirstCopyIsPublishedAndLaterCopyDropped(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.send(cloudGoTo("Hallway phone", model.Ptr(3)))
	if len(h.pub.events) != 1 || h.lock() != "LOCKING" {
		t.Fatalf("the first copy (cloud) must be published at once: %+v lock %s", h.pub.events, h.lock())
	}
	if n := h.pub.events[0].KeyName; n == nil || *n != "Hallway phone" {
		t.Fatalf("event %+v", h.pub.events[0])
	}
	h.advance(700 * time.Millisecond)
	h.send(bridgeGoTo(model.Ptr(3)))
	if len(h.pub.events) != 1 {
		t.Fatalf("the bridge copy is a duplicate: %+v", h.pub.events)
	}
}

func TestLaterCloudCopyNamesTheBridgeEvent(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.send(bridgeGoTo(model.Ptr(3)))
	h.advance(500 * time.Millisecond)
	h.send(cloudGoTo("Hallway phone", model.Ptr(3)))
	if len(h.pub.events) != 1 {
		t.Fatalf("events %+v", h.pub.events)
	}
	if n := h.state().LastKeyName; n == nil || *n != "Hallway phone" {
		t.Fatalf("state %+v", h.state())
	}
}

func TestCopiesWithOtherKeysAreSeparateEvents(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.send(cloudGoTo("Someone else", model.Ptr(5)))
	h.send(bridgeGoTo(model.Ptr(3)))
	if len(h.pub.events) != 2 || h.pub.events[1].KeyName != nil {
		t.Fatalf("events %+v", h.pub.events)
	}
}

// Only the latest event is compared (user decision): a repeat that arrives
// after another event is published again.
func TestOnlyTheLatestEventIsCompared(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.send(bridgeGoTo(model.Ptr(3)))
	h.send(reached("STATE_CHANGED_NIGHT_LOCK", model.Ptr(3)))
	h.advance(2 * time.Second)
	h.send(cloudGoTo("", model.Ptr(3)))
	if len(h.pub.events) != 3 {
		t.Fatalf("events %+v", h.pub.events)
	}
}

func TestDedupCanBeDisabled(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.s.t.DuplicateWindow = 0
	h.start()
	h.send(cloudGoTo("", model.Ptr(3)))
	h.send(bridgeGoTo(model.Ptr(3)))
	if len(h.pub.events) != 2 {
		t.Fatalf("every delivery must be published: %+v", h.pub.events)
	}
}

func TestDedupWindowIsConfigurable(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.s.t.DuplicateWindow = 2 * time.Second
	h.start()
	h.send(reached("STATE_CHANGED_NIGHT_LOCK", model.Ptr(3)))
	h.advance(3 * time.Second)
	h.send(cloudReached(""))
	if len(h.pub.events) != 2 {
		t.Fatalf("a repeat outside the window is a new event: %+v", h.pub.events)
	}
}

func TestDuplicateDeliveriesAreDropped(t *testing.T) {
	cases := []struct {
		name  string
		mode  func(*harness)
		event func() any
	}{
		{"bridge", func(*harness) {}, func() any { return reached("STATE_CHANGED_NIGHT_LOCK", model.Ptr(3)) }},
		{"cloud in cloud mode", (*harness).toCloud, func() any { return cloudReached("x") }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := newHarness(t, testRecord(), config.LockSetting{})
			h.start()
			c.mode(h)
			h.send(c.event())
			h.advance(4 * time.Second)
			states, events := len(h.pub.states), len(h.pub.events)
			h.send(c.event())
			if len(h.pub.states) != states || len(h.pub.events) != events {
				t.Fatalf("duplicate within 10 s published: events %+v", h.pub.events)
			}
			h.advance(11 * time.Second)
			h.send(c.event())
			if len(h.pub.events) != events+1 {
				t.Fatalf("a repeat after 10 s is a new event: %+v", h.pub.events)
			}
		})
	}
}

func TestDifferentEventsAreNotDuplicates(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.send(reached("STATE_CHANGED_NIGHT_LOCK", model.Ptr(3)))
	h.send(reached("STATE_CHANGED_NIGHT_LOCK", model.Ptr(4)))
	h.send(reached("STATE_CHANGED_LATCH", model.Ptr(4)))
	if len(h.pub.events) != 3 {
		t.Fatalf("events %+v", h.pub.events)
	}
}

// The lock's own return to day_lock after an open is a real state change:
// it is published like any other event (source unknown: no key).
func TestAutomaticLatchAfterOpenIsAnEvent(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.send(reached("STATE_CHANGED_OPEN", model.Ptr(3)))
	h.advance(3 * time.Second)
	h.send(reached("STATE_CHANGED_LATCH", nil))
	last := h.pub.events[len(h.pub.events)-1]
	if last.EventType != model.EventUnlocked || last.Source != model.SourceUnknown || h.lock() != "UNLOCKED" {
		t.Fatalf("event %+v lock %s", last, h.lock())
	}
}

func TestEventSources(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{}) // gateway key is local_id 1
	h.start()
	h.send(reached("STATE_CHANGED_LATCH", nil))
	if src := h.pub.events[0].Source; src != model.SourceUnknown {
		t.Fatalf("no key: %s", src)
	}
	h.advance(11 * time.Second)
	h.send(reached("STATE_CHANGED_NIGHT_LOCK_REMOTE", model.Ptr(1)))
	if src := h.pub.events[1].Source; src != "remote" {
		t.Fatalf("own key without a gateway command: %s", src)
	}
	h.s.lastCommandSentAt = h.now
	h.advance(59 * time.Second)
	h.send(reached("STATE_CHANGED_LATCH_REMOTE", model.Ptr(1)))
	if src := h.pub.events[2].Source; src != model.SourceGateway {
		t.Fatalf("own key within 60 s of a gateway command: %s", src)
	}
	h.advance(2 * time.Second)
	h.send(reached("STATE_CHANGED_NIGHT_LOCK_REMOTE", model.Ptr(1)))
	if src := h.pub.events[3].Source; src != "remote" {
		t.Fatalf("own key after the window: %s", src)
	}
	h.send(reached("STATE_CHANGED_LATCH_REMOTE", model.Ptr(2)))
	if src := h.pub.events[4].Source; src != "remote" {
		t.Fatalf("another key: %s", src)
	}
}

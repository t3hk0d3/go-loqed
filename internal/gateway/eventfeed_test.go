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

func TestCloudCopyFirstIsMergedIntoBridgeEvent(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	states := len(h.pub.states)
	h.send(cloudGoTo("Hallway phone", model.Ptr(3)))
	if len(h.pub.states) != states || len(h.pub.events) != 0 {
		t.Fatal("a cloud copy must wait for its bridge copy")
	}
	h.advance(700 * time.Millisecond)
	h.send(bridgeGoTo(model.Ptr(3)))
	if len(h.pub.events) != 1 {
		t.Fatalf("want exactly one event, got %+v", h.pub.events)
	}
	if n := h.pub.events[0].KeyName; n == nil || *n != "Hallway phone" {
		t.Fatalf("the cloud key name must name the bridge event: %+v", h.pub.events[0])
	}
	h.run(time.Minute)
	if len(h.pub.events) != 1 {
		t.Fatalf("the held copy must be consumed: %+v", h.pub.events)
	}
}

func TestHeldCloudCopyWithoutBridgeCopyIsDropped(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	states := len(h.pub.states)
	h.send(cloudGoTo("Hallway phone", model.Ptr(3)))
	h.run(31 * time.Second)
	if len(h.pub.states) != states || len(h.pub.events) != 0 {
		t.Fatalf("an unmatched cloud copy must never publish: %+v", h.pub.events)
	}
}

func TestCloudCopyWithOtherKeyIsNotMerged(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.send(cloudGoTo("Someone else", model.Ptr(5)))
	h.send(bridgeGoTo(model.Ptr(3)))
	if n := h.pub.events[0].KeyName; n != nil {
		t.Fatalf("a different key must not name the event: %s", *n)
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

package gateway

import (
	"slices"
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

// A late copy from the other feed is dropped even after a newer event.
func TestLateCopyFromOtherFeedIsDropped(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.send(bridgeGoTo(model.Ptr(3)))
	h.send(reached("STATE_CHANGED_NIGHT_LOCK", model.Ptr(3)))
	h.advance(2 * time.Second)
	h.send(cloudGoTo("", model.Ptr(3)))
	if len(h.pub.events) != 2 || h.lock() != "LOCKED" {
		t.Fatalf("events %+v lock %s", h.pub.events, h.lock())
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
	h.advance(1 * time.Second)
	h.send(reached("STATE_CHANGED_NIGHT_LOCK", model.Ptr(3)))
	h.advance(3 * time.Second)
	h.send(reached("STATE_CHANGED_NIGHT_LOCK", model.Ptr(3)))
	if len(h.pub.events) != 2 {
		t.Fatalf("a repeat within the window is dropped, one outside it is a new event: %+v", h.pub.events)
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
	if last.EventType != model.EventUnlocked || src(last) != "" || h.lock() != "UNLOCKED" {
		t.Fatalf("event %+v lock %s", last, h.lock())
	}
}

func TestEventSources(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{}) // gateway key is local_id 1
	h.start()
	h.send(reached("STATE_CHANGED_LATCH", nil))
	h.advance(11 * time.Second)
	h.send(reached("STATE_CHANGED_NIGHT_LOCK_REMOTE", model.Ptr(1)))
	h.s.lastCommandSentAt = h.now
	h.advance(59 * time.Second)
	h.send(reached("STATE_CHANGED_LATCH_REMOTE", model.Ptr(1)))
	h.advance(2 * time.Second)
	h.send(reached("STATE_CHANGED_NIGHT_LOCK_REMOTE", model.Ptr(1)))
	h.send(reached("STATE_CHANGED_LATCH_REMOTE", model.Ptr(2)))
	want := []string{"", "", model.SourceGateway, "", ""}
	for i, w := range want {
		if got := src(h.pub.events[i]); got != w {
			t.Errorf("event %d (%s key %v): source %q, want %q", i, h.pub.events[i].Reason, h.pub.events[i].KeyLocalID, got, w)
		}
	}
}

// Observed 2026-10-06 (PIN unlock): the bridge copies arrived 2 s late,
// after the cloud's next event.
func TestInterleavedCopiesArePublishedOnce(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	pin := func(feed string) any {
		if feed == "cloud" {
			return CloudEventMsg{Event: cloud.WebhookEvent{Kind: cloud.KindGoToState, EventType: "GO_TO_STATE_MANUAL_UNLOCK_VIA_OUTSIDE_MODULE_PIN",
				GoToState: loqed.BoltOpen, KeyLocalID: model.Ptr(1)}}
		}
		return BridgeEventMsg{Event: bridge.GoToStateEvent{EventType: "GO_TO_STATE_MANUAL_UNLOCK_VIA_OUTSIDE_MODULE_PIN",
			GoToState: loqed.BoltOpen, KeyLocalID: model.Ptr(1)}}
	}
	opened := CloudEventMsg{Event: cloud.WebhookEvent{Kind: cloud.KindStateReached, EventType: "STATE_CHANGED_OPEN",
		BoltState: loqed.BoltOpen, KeyLocalID: model.Ptr(1)}}
	h.send(pin("cloud"))
	h.advance(900 * time.Millisecond)
	h.send(opened)
	h.advance(1100 * time.Millisecond)
	h.send(pin("bridge"))
	h.send(reached("STATE_CHANGED_OPEN", model.Ptr(1)))
	var got []model.EventType
	for _, e := range h.pub.events {
		got = append(got, e.EventType)
	}
	if !slices.Equal(got, []model.EventType{model.EventOpening, model.EventOpened}) || h.lock() != "OPEN" {
		t.Fatalf("events %v lock %s", got, h.lock())
	}
}

// Knob jiggling repeats the same event: noise, dropped. A real change back
// is not a repeat and is published, so the state stays right.
func TestRepeatsAreDroppedButChangesBackArePublished(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	both := func(eventType string) {
		h.send(CloudEventMsg{Event: cloud.WebhookEvent{Kind: cloud.KindStateReached, EventType: eventType,
			BoltState: mustReached(eventType)}})
		h.advance(300 * time.Millisecond)
		h.send(reached(eventType, nil))
		h.advance(2 * time.Second)
	}
	both("STATE_CHANGED_LATCH")
	both("STATE_CHANGED_LATCH") // jiggle
	both("STATE_CHANGED_NIGHT_LOCK")
	both("STATE_CHANGED_LATCH")
	var got []model.EventType
	for _, e := range h.pub.events {
		got = append(got, e.EventType)
	}
	want := []model.EventType{model.EventUnlocked, model.EventLocked, model.EventUnlocked}
	if !slices.Equal(got, want) || h.lock() != "UNLOCKED" {
		t.Fatalf("events %v, want %v; lock %s", got, want, h.lock())
	}
}

func mustReached(eventType string) loqed.BoltState {
	b, _ := loqed.ReachedState(eventType)
	return b
}

// Bolt-state events without a key, from either feed.
func viaCloud(eventType string) CloudEventMsg {
	return CloudEventMsg{Event: cloud.WebhookEvent{Kind: cloud.KindStateReached, EventType: eventType, BoltState: mustReached(eventType)}}
}

func viaBridge(eventType string) BridgeEventMsg { return reached(eventType, nil) }

func (h *harness) eventTypes() []model.EventType {
	var got []model.EventType
	for _, e := range h.pub.events {
		got = append(got, e.EventType)
	}
	return got
}

func TestLateCopyAfterDedupWindowIsDropped(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.send(viaCloud("STATE_CHANGED_NIGHT_LOCK"))
	h.advance(2 * time.Minute)
	h.send(viaBridge("STATE_CHANGED_NIGHT_LOCK"))
	if len(h.pub.events) != 1 || h.lock() != "LOCKED" {
		t.Fatalf("events %v lock %s", h.eventTypes(), h.lock())
	}
}

func TestPairingAbsorbsOnlyOneCopy(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.send(viaCloud("STATE_CHANGED_NIGHT_LOCK"))
	h.advance(2 * time.Minute)
	h.send(viaBridge("STATE_CHANGED_NIGHT_LOCK")) // its copy
	h.advance(time.Minute)
	h.send(viaBridge("STATE_CHANGED_NIGHT_LOCK")) // a new event
	if len(h.pub.events) != 2 {
		t.Fatalf("events %v", h.eventTypes())
	}
}

func TestSameFeedRepeatAfterWindowIsPublished(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.send(viaBridge("STATE_CHANGED_NIGHT_LOCK"))
	h.advance(30 * time.Second)
	h.send(viaBridge("STATE_CHANGED_NIGHT_LOCK"))
	if len(h.pub.events) != 2 {
		t.Fatalf("events %v", h.eventTypes())
	}
}

func TestCopyAfterPairingWindowIsPublished(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.send(viaCloud("STATE_CHANGED_NIGHT_LOCK"))
	h.advance(6 * time.Minute)
	h.send(viaBridge("STATE_CHANGED_NIGHT_LOCK"))
	if len(h.pub.events) != 2 {
		t.Fatalf("events %v", h.eventTypes())
	}
}

// The bridge lags by minutes: its copies must not roll the state back.
func TestInterleavedLateFeedIsDropped(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.send(viaCloud("STATE_CHANGED_LATCH"))
	h.advance(30 * time.Second)
	h.send(viaCloud("STATE_CHANGED_NIGHT_LOCK"))
	h.advance(90 * time.Second)
	h.send(viaBridge("STATE_CHANGED_LATCH"))
	h.advance(time.Second)
	h.send(viaBridge("STATE_CHANGED_NIGHT_LOCK"))
	want := []model.EventType{model.EventUnlocked, model.EventLocked}
	if !slices.Equal(h.eventTypes(), want) || h.lock() != "LOCKED" {
		t.Fatalf("events %v lock %s", h.eventTypes(), h.lock())
	}
}

// Day's bridge copy is lost; both feeds deliver night; then a real day
// reaches the bridge first. Once the bridge delivered night, the old cloud
// day can no longer be paired, so the real day is published.
func TestPairingWindowNeverDropsARealChangeBack(t *testing.T) {
	for _, bridgeFirst := range []bool{false, true} {
		h := newHarness(t, testRecord(), config.LockSetting{})
		h.start()
		h.send(viaCloud("STATE_CHANGED_LATCH"))
		h.advance(30 * time.Second)
		if bridgeFirst {
			h.send(viaBridge("STATE_CHANGED_NIGHT_LOCK"))
			h.advance(300 * time.Millisecond)
			h.send(viaCloud("STATE_CHANGED_NIGHT_LOCK"))
		} else {
			h.send(viaCloud("STATE_CHANGED_NIGHT_LOCK"))
			h.advance(300 * time.Millisecond)
			h.send(viaBridge("STATE_CHANGED_NIGHT_LOCK"))
		}
		h.advance(2 * time.Minute)
		h.send(viaBridge("STATE_CHANGED_LATCH"))
		want := []model.EventType{model.EventUnlocked, model.EventLocked, model.EventUnlocked}
		if !slices.Equal(h.eventTypes(), want) || h.lock() != "UNLOCKED" {
			t.Fatalf("bridgeFirst=%v: events %v lock %s", bridgeFirst, h.eventTypes(), h.lock())
		}
	}
}

func TestSingleFeedChangesBackArePublished(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	for _, et := range []string{"STATE_CHANGED_LATCH", "STATE_CHANGED_NIGHT_LOCK", "STATE_CHANGED_LATCH", "STATE_CHANGED_NIGHT_LOCK"} {
		h.send(viaBridge(et))
		h.advance(time.Minute)
	}
	if len(h.pub.events) != 4 {
		t.Fatalf("events %v", h.eventTypes())
	}
}

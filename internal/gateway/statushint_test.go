package gateway

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/bridge"
	"github.com/t3hk0d3/go-loqed/cloud"
	"github.com/t3hk0d3/go-loqed/internal/config"
	"github.com/t3hk0d3/go-loqed/internal/model"
)

// statusRead runs the periodic reconcile right now.
func (h *harness) statusRead() {
	h.t.Helper()
	h.s.nextReconcile = h.now
	h.advance(0)
}

func TestStatusAppliedWithoutRecentWebhook(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.s.state.StateStale = true
	h.bridge.status.BoltState = loqed.BoltNightLock
	h.statusRead()
	if s := h.state(); s.BoltState != loqed.BoltNightLock || h.lock() != "LOCKED" || s.StateStale {
		t.Fatalf("state %+v", s)
	}
}

func TestStatusNeverOverridesRecentWebhook(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.send(reached("STATE_CHANGED_NIGHT_LOCK", model.Ptr(3)))
	h.bridge.status.BoltState = loqed.BoltDayLock // /status lags behind
	h.advance(4 * time.Minute)
	h.statusRead()
	if s := h.state(); s.BoltState != loqed.BoltNightLock || h.lock() != "LOCKED" || s.StateStale {
		t.Fatalf("a lagging /status must not undo a webhook: %+v", s)
	}
	h.advance(2 * time.Minute) // the webhook is now older than 5 min
	h.statusRead()
	if s := h.state(); s.BoltState != loqed.BoltDayLock {
		t.Fatalf("older webhook state must yield to /status: %+v", s)
	}
}

func TestStatusShowingTheMovementTargetIsApplied(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.send(reached("STATE_CHANGED_LATCH", model.Ptr(3)))
	h.send(goTo("GO_TO_STATE_TOUCH_TO_LOCK", loqed.BoltNightLock))
	h.bridge.status.BoltState = loqed.BoltNightLock
	h.advance(10 * time.Second)
	h.statusRead()
	if s := h.state(); s.BoltState != loqed.BoltNightLock || h.lock() != "LOCKED" || s.StateStale {
		t.Fatalf("state %+v", s)
	}
	if h.s.move.active {
		t.Fatal("the movement must be resolved")
	}
}

func TestStatusStillShowingPreviousStateIsInconclusive(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start() // day_lock from /status
	h.send(goTo("GO_TO_STATE_TOUCH_TO_LOCK", loqed.BoltNightLock))
	h.advance(time.Minute)
	h.statusRead() // still day_lock
	s := h.state()
	if s.BoltState != loqed.BoltDayLock || h.lock() != "LOCKING" || !s.StateStale {
		t.Fatalf("an inconclusive read must keep the state and mark it stale: %+v lock %s", s, h.lock())
	}
	h.send(reached("STATE_CHANGED_NIGHT_LOCK", model.Ptr(3)))
	if s := h.state(); s.StateStale || h.lock() != "LOCKED" {
		t.Fatalf("a webhook resolves it: %+v", s)
	}
}

func TestStatusAlwaysAppliesBatteryAndSignal(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.send(reached("STATE_CHANGED_NIGHT_LOCK", model.Ptr(3)))
	h.bridge.status = bridge.Status{BoltState: loqed.BoltDayLock, LockOnline: 1, BatteryPercentage: 41, BatteryVoltage: 9.9,
		WifiStrength: 30, BLEStrength: 95}
	h.statusRead()
	s := h.state()
	if *s.BatteryPercentage != 41 || *s.BatteryVoltage != 9.9 || *s.WifiStrength != 30 || *s.BLEStrength != 95 || !s.LockOnline {
		t.Fatalf("state %+v", s)
	}
	if s.BoltState != loqed.BoltNightLock {
		t.Fatal("the bolt must still come from the webhook")
	}
}

func TestStatusReplacesUnknownBoltFromWebhook(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.send(reached("STATE_CHANGED_UNKNOWN", nil))
	h.bridge.status.BoltState = loqed.BoltDayLock
	h.statusRead()
	if h.state().BoltState != loqed.BoltDayLock {
		t.Fatalf("a known /status bolt beats an unknown webhook bolt: %+v", h.state())
	}
}

func TestGoToWithoutStateChangedReadsStatusTwice(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start() // day_lock
	h.send(goTo("GO_TO_STATE_TOUCH_TO_LOCK", loqed.BoltNightLock))
	h.run(29 * time.Second)
	if h.bridge.statusCalls != 1 {
		t.Fatalf("too early: %d", h.bridge.statusCalls)
	}
	h.run(time.Second)
	if h.bridge.statusCalls != 2 {
		t.Fatalf("expected the 30 s read: %d", h.bridge.statusCalls)
	}
	h.run(59 * time.Second)
	if h.bridge.statusCalls != 2 {
		t.Fatalf("recheck too early: %d", h.bridge.statusCalls)
	}
	h.run(time.Second)
	if h.bridge.statusCalls != 3 {
		t.Fatalf("expected the recheck 60 s later: %d", h.bridge.statusCalls)
	}
	h.run(5 * time.Minute)
	if h.bridge.statusCalls != 3 {
		t.Fatalf("no further reads: %d", h.bridge.statusCalls)
	}
}

func TestGoToResolvedByStatusIsNotRechecked(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.send(goTo("GO_TO_STATE_TOUCH_TO_LOCK", loqed.BoltNightLock))
	h.bridge.status.BoltState = loqed.BoltNightLock
	h.run(2 * time.Minute)
	if h.bridge.statusCalls != 2 || h.lock() != "LOCKED" {
		t.Fatalf("status %d lock %s", h.bridge.statusCalls, h.lock())
	}
}

func TestStateChangedCancelsConfirmationReads(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.send(goTo("GO_TO_STATE_TOUCH_TO_LOCK", loqed.BoltNightLock))
	h.run(5 * time.Second)
	h.send(reached("STATE_CHANGED_NIGHT_LOCK", model.Ptr(3)))
	h.run(3 * time.Minute)
	if h.bridge.statusCalls != 1 {
		t.Fatalf("status calls %d", h.bridge.statusCalls)
	}
}

func TestMotorStallReadsStatusOnceAfterThirtySeconds(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.send(reached("MOTOR_STALL", model.Ptr(1)))
	h.run(29 * time.Second)
	if h.bridge.statusCalls != 1 {
		t.Fatal("too early")
	}
	h.run(time.Second)
	if h.bridge.statusCalls != 2 || h.lock() != "UNLOCKED" {
		t.Fatalf("status %d lock %s", h.bridge.statusCalls, h.lock())
	}
	h.run(5 * time.Minute)
	if h.bridge.statusCalls != 2 {
		t.Fatalf("read once only: %d", h.bridge.statusCalls)
	}
}

func TestLaterEventsBringTheLockBackOnline(t *testing.T) {
	bridgeOffline := BridgeEventMsg{Event: bridge.OnlineEvent{BLEStrength: model.Ptr(-1)}}
	cloudOffline := CloudEventMsg{Event: cloud.WebhookEvent{Kind: cloud.KindSignal, LockID: "lock1", BLEStrength: model.Ptr(-1)}}
	cases := []struct {
		name    string
		cloud   bool // cloud events change state only in cloud mode
		offline any
		ev      any
	}{
		{"bridge state", false, bridgeOffline, reached("STATE_CHANGED_LATCH", nil)},
		{"bridge signal without ble", false, bridgeOffline, BridgeEventMsg{Event: bridge.OnlineEvent{WifiStrength: model.Ptr(30)}}},
		{"cloud battery", true, cloudOffline, CloudEventMsg{Event: cloud.WebhookEvent{Kind: cloud.KindSignal, LockID: "lock1", BatteryPercentage: model.Ptr(70)}}},
		{"cloud signal", true, cloudOffline, CloudEventMsg{Event: cloud.WebhookEvent{Kind: cloud.KindSignal, LockID: "lock1", BLEStrength: model.Ptr(90)}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := newHarness(t, testRecord(), config.LockSetting{}, func(d *Deps) { d.CloudWebhooks = true })
			h.start()
			if c.cloud {
				h.toCloud()
			}
			h.send(c.offline)
			if h.state().LockOnline {
				t.Fatal("ble -1 must mark the lock offline")
			}
			h.send(c.ev)
			if !h.state().LockOnline || !h.available() {
				t.Fatalf("state %+v", h.state())
			}
		})
	}
}

func TestManyBridgeWebhooksAreReported(t *testing.T) {
	for _, c := range []struct {
		others int
		warn   bool
	}{{3, false}, {4, true}} {
		var buf bytes.Buffer
		h := newHarness(t, testRecord(), config.LockSetting{}, func(d *Deps) { d.Log = slog.New(slog.NewTextHandler(&buf, nil)) })
		for i := range c.others {
			h.bridge.hooks = append(h.bridge.hooks, bridge.Webhook{ID: loqed.Int(i + 1), URL: "http://192.0.2.99/hook" + string(rune('a'+i))})
		}
		h.start()
		if got := strings.Contains(buf.String(), "webhook target delays"); got != c.warn {
			t.Errorf("%d other webhooks: warned=%v want %v: %s", c.others, got, c.warn, buf.String())
		}
	}
}

package gateway

import (
	"context"
	"errors"
	"testing"
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/bridge"
	"github.com/t3hk0d3/go-loqed/internal/config"
	"github.com/t3hk0d3/go-loqed/internal/model"
)

func reached(eventType string, key *int) BridgeEventMsg {
	b, jammed := loqed.ReachedState(eventType)
	return BridgeEventMsg{Event: bridge.StateReachedEvent{EventType: eventType, BoltState: b, Jammed: jammed, KeyLocalID: key}}
}

func goTo(eventType string, target loqed.BoltState) BridgeEventMsg {
	return BridgeEventMsg{Event: bridge.GoToStateEvent{EventType: eventType, GoToState: target, KeyLocalID: model.Ptr(3)}}
}

func TestStartsLocalAndRegistersWebhook(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	if h.s.mode != model.ModeLocal || h.lock() != "UNLOCKED" || !h.available() || h.state().Mode != model.ModeLocal {
		t.Fatalf("mode %s lock %s", h.s.mode, h.lock())
	}
	if len(h.bridge.created) != 1 || h.bridge.created[0] != "http://10.0.0.5:8099/webhook/lock1" || !h.s.webhookOK {
		t.Fatalf("created %v", h.bridge.created)
	}
	if h.state().BatteryPercentage == nil || *h.state().BatteryPercentage != 80 || h.state().StateStale {
		t.Fatalf("state %+v", h.state())
	}
}

func TestKeepsCurrentWebhookAndDeletesStaleOnes(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.bridge.hooks = []bridge.Webhook{
		{ID: 1, URL: "http://10.0.0.9:8099/webhook/lock1"},      // old gateway IP
		{ID: 2, URL: "http://10.0.0.5:8099/webhook/lock1"},      // current
		{ID: 3, URL: "http://ha.local:8123/api/webhook/abcdef"}, // someone else's
	}
	h.start()
	if len(h.bridge.created) != 0 || len(h.bridge.deleted) != 1 || h.bridge.deleted[0] != 1 {
		t.Fatalf("created %v deleted %v", h.bridge.created, h.bridge.deleted)
	}
}

func TestBridgeEventsUpdateStateAndEmitEvents(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.send(goTo("GO_TO_STATE_TOUCH_TO_LOCK", loqed.BoltNightLock))
	if h.lock() != "LOCKING" || h.pub.events[0].EventType != model.EventLocking || h.pub.events[0].Source != "touch" {
		t.Fatalf("lock %s events %+v", h.lock(), h.pub.events)
	}
	h.send(reached("STATE_CHANGED_NIGHT_LOCK", nil)) // the library maps 255 to nil
	s := h.state()
	if h.lock() != "LOCKED" || s.BoltState != loqed.BoltNightLock || s.LastEvent != "STATE_CHANGED_NIGHT_LOCK" || s.LastKeyID != nil || s.LastEventAt == nil {
		t.Fatalf("state %+v", s)
	}
	if ev := h.pub.events[1]; ev.EventType != model.EventLocked || ev.KeyLocalID != nil || ev.KeyName != nil {
		t.Fatalf("event %+v", ev)
	}
}

func TestKeyNamesFromSettings(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{KeyNames: config.KeyNames{3: "Alice"}})
	h.start()
	h.send(reached("STATE_CHANGED_LATCH", model.Ptr(3)))
	if n := h.state().LastKeyName; n == nil || *n != "Alice" {
		t.Fatalf("state %+v", h.state())
	}
	if n := h.pub.events[0].KeyName; n == nil || *n != "Alice" {
		t.Fatalf("event %+v", h.pub.events[0])
	}
}

func TestOnlineAndBatteryEventsTrackLockOnline(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.send(BridgeEventMsg{Event: bridge.OnlineEvent{WifiStrength: model.Ptr(70), BLEStrength: model.Ptr(-1)}})
	if h.state().LockOnline || h.available() {
		t.Fatal("ble -1 must mark the lock offline")
	}
	h.send(BridgeEventMsg{Event: bridge.BatteryEvent{BatteryPercentage: 55}})
	if !h.state().LockOnline || *h.state().BatteryPercentage != 55 || !h.available() {
		t.Fatalf("battery report must mark the lock online: %+v", h.state())
	}
	h.send(BridgeEventMsg{Event: bridge.BatteryEvent{BatteryPercentage: -1}})
	if h.state().LockOnline || *h.state().BatteryPercentage != 55 {
		t.Fatalf("battery -1 means offline and must not overwrite the level: %+v", h.state())
	}
	if len(h.pub.events) != 0 {
		t.Fatal("battery/online reports must not emit lock events")
	}
}

func TestStatusOnlyOnReconcileSchedule(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	if h.bridge.statusCalls != 1 {
		t.Fatalf("status calls %d", h.bridge.statusCalls)
	}
	for range 10 {
		h.advance(60 * time.Second)
	}
	if h.bridge.statusCalls != 1 || len(h.probes) != 10 || h.probes[0] != "192.0.2.10:80" {
		t.Fatalf("status %d probes %v", h.bridge.statusCalls, h.probes)
	}
	h.advance(24 * time.Hour)
	if h.bridge.statusCalls != 2 {
		t.Fatalf("expected daily reconcile, status calls %d", h.bridge.statusCalls)
	}
}

func TestWebhookCountsAsLiveness(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.advance(50 * time.Second)
	h.send(BridgeEventMsg{Event: bridge.OnlineEvent{BLEStrength: model.Ptr(40)}})
	h.advance(20 * time.Second) // 70s after start, 20s after the webhook
	if len(h.probes) != 0 {
		t.Fatalf("probe should be postponed by the webhook: %v", h.probes)
	}
}

func TestUnknownBoltRecheckedEveryTenMinutes(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.bridge.status.BoltState = loqed.BoltUnknown
	h.start()
	if h.state().Lock != nil {
		t.Fatal("unknown bolt must publish lock=null")
	}
	h.advance(5 * time.Minute)
	if h.bridge.statusCalls != 1 {
		t.Fatalf("too early: %d", h.bridge.statusCalls)
	}
	h.advance(5 * time.Minute)
	if h.bridge.statusCalls != 2 {
		t.Fatalf("expected recheck: %d", h.bridge.statusCalls)
	}
}

func TestBridgeAuthErrorRefreshesCredentials(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.bridge.listErr = loqed.ErrUnauthorized
	h.start()
	if len(h.refreshes) != 1 || h.refreshes[0] != ReasonUnauthorized || h.bridgesBuilt != 2 {
		t.Fatalf("refreshes %v bridges %d", h.refreshes, h.bridgesBuilt)
	}
	if h.s.mode != model.ModeLocal {
		t.Fatal("status works without keys; stay local")
	}
}

func TestPinnedKeysSkipAuthRefresh(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{BridgeKey: "Ym9uam91ciBtb25kZQ=="})
	h.bridge.listErr = loqed.ErrUnauthorized
	h.start()
	if len(h.refreshes) != 0 {
		t.Fatalf("a refresh cannot fix pinned keys: %v", h.refreshes)
	}
}

// A STATE_CHANGED webhook can be lost; GO_TO_STATE alone must not leave
// HA showing LOCKING until the daily reconcile.
func TestLostStateChangedAfterGoToTriggersStatus(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.bridge.status.BoltState = loqed.BoltNightLock
	h.send(goTo("GO_TO_STATE_TOUCH_TO_LOCK", loqed.BoltNightLock))
	if h.lock() != "LOCKING" {
		t.Fatalf("lock %s", h.lock())
	}
	h.run(29 * time.Second)
	if h.bridge.statusCalls != 1 {
		t.Fatal("too early")
	}
	h.run(2 * time.Second)
	if h.bridge.statusCalls != 2 || h.lock() != "LOCKED" {
		t.Fatalf("status %d lock %s", h.bridge.statusCalls, h.lock())
	}
}

func TestMotorStallSchedulesStatus(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.send(reached("MOTOR_STALL", model.Ptr(1)))
	if h.lock() != "JAMMED" {
		t.Fatalf("lock %s", h.lock())
	}
	h.run(31 * time.Second)
	if h.bridge.statusCalls != 2 || h.lock() != "UNLOCKED" {
		t.Fatalf("status %d lock %s", h.bridge.statusCalls, h.lock())
	}
}

func TestWebhookRegistrationRetriedEveryTenMinutes(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.bridge.listErr = errors.New("bridge has no free webhook slots")
	h.start()
	if h.s.mode != model.ModeLocal || h.s.webhookOK || h.bridge.listCalls != 1 {
		t.Fatalf("mode %s ok %v lists %d", h.s.mode, h.s.webhookOK, h.bridge.listCalls)
	}
	h.advance(10 * time.Minute)
	if h.bridge.listCalls != 2 || h.bridge.statusCalls != 2 {
		t.Fatalf("retry + poll expected: lists %d status %d", h.bridge.listCalls, h.bridge.statusCalls)
	}
	h.bridge.listErr = nil
	h.advance(10 * time.Minute)
	if !h.s.webhookOK || len(h.bridge.created) != 1 {
		t.Fatalf("ok %v created %v", h.s.webhookOK, h.bridge.created)
	}
	status := h.bridge.statusCalls
	h.advance(10 * time.Minute)
	if h.bridge.statusCalls != status {
		t.Fatal("polling must stop once the webhook is registered")
	}
}

// A refresh that returns unusable credentials must not leave a nil bridge
// client in local mode (that panicked and crash-looped the process).
func TestUnusableRefreshLeavesLocalWithoutPanic(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	broken := testRecord()
	broken.LocalID = nil // the cloud stopped reporting local credentials
	h.refreshRec = &broken
	h.bridge.listErr = loqed.ErrUnauthorized // keys rotated
	h.start()
	if h.s.mode == model.ModeLocal || h.s.bridge != nil {
		t.Fatalf("mode %s bridge %v", h.s.mode, h.s.bridge)
	}
	h.run(30 * time.Second) // ticks must not touch the nil bridge
}

// TCP up but HTTP hung: probes succeed, requests time out. The HTTP failure
// count must not be reset by the probe.
func TestHungHTTPFailsOverDespiteTCPProbe(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.bridge.status.BoltState = loqed.BoltUnknown // causes /status every 10 min
	h.start()
	h.bridge.statusErr = loqed.ErrNoResponse
	for range 3 {
		h.advance(10 * time.Minute)
	}
	stale := false
	for _, st := range h.pub.states {
		stale = stale || st.StateStale
	}
	if h.s.mode != model.ModeCloud || !stale {
		t.Fatalf("mode %s; failed /status must have marked the state stale: %v", h.s.mode, stale)
	}
}

func TestBridgeEventAppliedWhenLocalEntryFails(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.toCloud()
	h.bridge.statusErr = loqed.ErrNoResponse
	h.send(reached("STATE_CHANGED_LATCH", model.Ptr(2)))
	if h.s.mode != model.ModeCloud || h.lock() != "UNLOCKED" {
		t.Fatalf("mode %s lock %s", h.s.mode, h.lock())
	}
}

func TestBridgeAddress(t *testing.T) {
	if BridgeAddress("192.0.2.10") != "192.0.2.10:80" || BridgeAddress("127.0.0.1:8080") != "127.0.0.1:8080" {
		t.Fatal("BridgeAddress")
	}
}

// A bridge can lose its webhook (factory reset, slot cleanup); the periodic
// reconcile must put it back.
func TestLostBridgeWebhookIsRecreatedAtReconcile(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	if len(h.bridge.created) != 1 || !h.s.webhookOK {
		t.Fatalf("created %v", h.bridge.created)
	}
	h.bridge.hooks = nil
	h.advance(24 * time.Hour)
	if len(h.bridge.created) != 2 {
		t.Fatalf("webhook not re-created: %v", h.bridge.created)
	}
}

// An unrecognized event carries no state, so it cannot make stale data fresh.
func TestUnrecognizedEventKeepsStateStale(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.s.markStale()
	h.send(reached("SOMETHING_ELSE", nil))
	if !h.state().StateStale {
		t.Fatal("an event without state cleared state_stale")
	}
	h.send(reached("STATE_CHANGED_LATCH", nil))
	if h.state().StateStale {
		t.Fatal("a state event must clear state_stale")
	}
}

func TestTickDoesNothingAfterShutdown(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	h.now = h.now.Add(24 * time.Hour)
	probes, status := len(h.probes), h.bridge.statusCalls
	h.s.tick(ctx)
	if len(h.probes) != probes || h.bridge.statusCalls != status {
		t.Fatal("tick issued requests after shutdown")
	}
}

package gateway

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/bridge"
	"github.com/t3hk0d3/go-loqed/cloud"
	"github.com/t3hk0d3/go-loqed/loqed-mqtt/internal/config"
	"github.com/t3hk0d3/go-loqed/loqed-mqtt/internal/model"
)

const undeliveredMsg = "bridge webhooks are not reaching the gateway"

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

// loggedHarness is a harness whose supervisor logs (warn and up) into buf.
func loggedHarness(t *testing.T) (*harness, *syncBuffer) {
	buf := &syncBuffer{}
	h := newHarness(t, testRecord(), config.LockSetting{}, func(d *Deps) {
		d.Log = slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	})
	return h, buf
}

func onlineEvent() BridgeEventMsg {
	return BridgeEventMsg{Event: bridge.OnlineEvent{BLEStrength: model.Ptr(40)}}
}

// confirmDelivery delivers a signed bridge webhook.
func (h *harness) confirmDelivery() {
	h.t.Helper()
	h.send(onlineEvent())
	if !h.s.webhookConfirmed {
		h.t.Fatal("a bridge webhook must confirm delivery")
	}
}

func TestEnteringLocalLeavesDeliveryUnconfirmed(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	if h.s.mode != model.ModeLocal || h.s.webhookConfirmed {
		t.Fatalf("mode %s confirmed %v", h.s.mode, h.s.webhookConfirmed)
	}
}

func TestBridgeWebhookConfirmsDelivery(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.advance(10 * time.Second)
	h.confirmDelivery()
	if !h.s.lastBridgeEventAt.Equal(h.now) {
		t.Fatalf("lastBridgeEventAt %v, now %v", h.s.lastBridgeEventAt, h.now)
	}
}

func TestDuplicateBridgeWebhookConfirmsDelivery(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.send(cloudReached(""))
	h.send(reached("STATE_CHANGED_NIGHT_LOCK", model.Ptr(3))) // the bridge copy, dropped
	if len(h.pub.events) != 1 || !h.s.webhookConfirmed {
		t.Fatalf("events %d confirmed %v", len(h.pub.events), h.s.webhookConfirmed)
	}
}

func TestUnconfirmedReadsStatusEveryLiveness(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.run(5 * time.Minute)
	if h.bridge.statusCalls != 6 {
		t.Fatalf("status calls %d, want 1 + one per minute", h.bridge.statusCalls)
	}
	h.confirmDelivery()
	h.run(10 * time.Minute)
	if h.bridge.statusCalls != 6 {
		t.Fatalf("reads continued after a bridge webhook: %d", h.bridge.statusCalls)
	}
}

func TestConfirmedReadsStatusOnlyAtReconcile(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.confirmDelivery()
	h.run(10 * time.Minute)
	if h.bridge.statusCalls != 1 {
		t.Fatalf("status calls %d", h.bridge.statusCalls)
	}
	h.advance(24 * time.Hour)
	if h.bridge.statusCalls != 2 {
		t.Fatalf("expected the daily reconcile: %d", h.bridge.statusCalls)
	}
}

func TestUnconfirmedReadsUseNoCloudBudget(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.run(30 * time.Minute)
	if len(h.cloud.calls) != 0 {
		t.Fatalf("cloud calls %v", h.cloud.calls)
	}
}

func TestUnconfirmedReadsApplyHintRules(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.send(cloudReached(""))                      // night by webhook
	h.bridge.status.BoltState = loqed.BoltDayLock // /status lags
	h.run(2 * time.Minute)
	if h.bridge.statusCalls < 2 || h.lock() != "LOCKED" {
		t.Fatalf("status calls %d lock %s", h.bridge.statusCalls, h.lock())
	}
}

func TestCreatedWebhookResetsConfirmation(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.confirmDelivery()
	h.bridge.hooks = nil // the bridge dropped our webhook
	h.advance(24 * time.Hour)
	if len(h.bridge.created) != 2 || h.s.webhookConfirmed {
		t.Fatalf("created %v confirmed %v", h.bridge.created, h.s.webhookConfirmed)
	}
}

func TestExistingWebhookKeepsConfirmation(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.confirmDelivery()
	h.advance(24 * time.Hour)
	if len(h.bridge.created) != 1 || !h.s.webhookConfirmed {
		t.Fatalf("created %v confirmed %v", h.bridge.created, h.s.webhookConfirmed)
	}
}

func TestRegistrationFailureReadsStatusEveryLiveness(t *testing.T) {
	h, logs := loggedHarness(t)
	h.bridge.listErr = errors.New("bridge has no free webhook slots")
	h.start()
	if !strings.Contains(logs.String(), "every minute") {
		t.Fatalf("log: %s", logs)
	}
	h.run(5 * time.Minute)
	if h.bridge.statusCalls != 6 || h.bridge.listCalls != 1 {
		t.Fatalf("status %d lists %d", h.bridge.statusCalls, h.bridge.listCalls)
	}
	h.run(5 * time.Minute)
	if h.bridge.statusCalls != 11 || h.bridge.listCalls != 2 {
		t.Fatalf("status %d lists %d", h.bridge.statusCalls, h.bridge.listCalls)
	}
}

// missedChange sets up a confirmed lock whose last bridge webhook was at
// lastBridge, and a /status that now shows night instead of day.
func missedChange(t *testing.T, lastBridge time.Duration) (*harness, *syncBuffer) {
	h, logs := loggedHarness(t)
	h.start()
	h.s.webhookConfirmed = true
	if lastBridge > 0 {
		h.s.lastBridgeEventAt = h.now.Add(-lastBridge)
	}
	h.bridge.status.BoltState = loqed.BoltNightLock
	h.s.reconcile(context.Background())
	return h, logs
}

func TestMissedChangeUnconfirmsDelivery(t *testing.T) {
	for _, last := range []time.Duration{0, 25 * time.Hour} {
		h, logs := missedChange(t, last)
		if h.s.webhookConfirmed {
			t.Fatalf("last=%v: still confirmed", last)
		}
		if n := strings.Count(logs.String(), undeliveredMsg); n != 1 || !strings.Contains(logs.String(), "address=10.0.0.5:8099") {
			t.Fatalf("last=%v: log %s", last, logs)
		}
	}
}

func TestChangeWithRecentBridgeWebhookKeepsConfirmation(t *testing.T) {
	h, logs := missedChange(t, time.Hour)
	if !h.s.webhookConfirmed || strings.Contains(logs.String(), undeliveredMsg) {
		t.Fatalf("confirmed %v log %s", h.s.webhookConfirmed, logs)
	}
}

func TestUnknownBoltDoesNotUnconfirm(t *testing.T) {
	h, logs := loggedHarness(t)
	h.bridge.status.BoltState = loqed.BoltUnknown
	h.start()
	h.s.webhookConfirmed = true
	h.bridge.status.BoltState = loqed.BoltNightLock
	h.s.reconcile(context.Background())
	if !h.s.webhookConfirmed || strings.Contains(logs.String(), undeliveredMsg) {
		t.Fatalf("confirmed %v log %s", h.s.webhookConfirmed, logs)
	}
}

// sendLocalLock sends LOCK via the bridge with delivery confirmed, and
// confirms it with a cloud webhook.
func sendLocalLock(t *testing.T) (*harness, *syncBuffer) {
	h, logs := loggedHarness(t)
	h.start()
	h.confirmDelivery()
	h.advance(time.Second)
	h.cmd(model.CommandLock, "")
	if st := h.lastStatus(); st.Status != model.StatusSent || *st.Via != model.ViaLocal {
		t.Fatalf("status %+v", st)
	}
	h.send(CloudEventMsg{Event: cloud.WebhookEvent{Kind: cloud.KindStateReached, EventType: "STATE_CHANGED_NIGHT_LOCK",
		BoltState: loqed.BoltNightLock, KeyLocalID: ourKey}})
	return h, logs
}

func TestLocalCommandWithoutBridgeWebhookUnconfirms(t *testing.T) {
	h, logs := sendLocalLock(t)
	h.run(29 * time.Second)
	if !h.s.webhookConfirmed {
		t.Fatal("unconfirmed before the confirmation window ended")
	}
	h.run(2 * time.Second)
	if h.s.webhookConfirmed || strings.Count(logs.String(), undeliveredMsg) != 1 {
		t.Fatalf("confirmed %v log %s", h.s.webhookConfirmed, logs)
	}
}

func TestLocalCommandWithBridgeWebhookKeepsConfirmation(t *testing.T) {
	h, logs := sendLocalLock(t)
	h.advance(2 * time.Second)
	h.send(reached("STATE_CHANGED_NIGHT_LOCK", ourKey)) // the bridge copy
	h.run(40 * time.Second)
	if !h.s.webhookConfirmed || strings.Contains(logs.String(), undeliveredMsg) {
		t.Fatalf("confirmed %v log %s", h.s.webhookConfirmed, logs)
	}
}

func TestCloudCommandSchedulesNoBridgeCheck(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.toCloud()
	h.cmd(model.CommandLock, "")
	if st := h.lastStatus(); st.Status != model.StatusSent || *st.Via != model.ViaCloud {
		t.Fatalf("status %+v", st)
	}
	if !h.s.bridgeCheckAt.IsZero() {
		t.Fatalf("bridge check scheduled at %v", h.s.bridgeCheckAt)
	}
}

func TestUnconfirmedWarningIsRateLimited(t *testing.T) {
	h, logs := loggedHarness(t)
	h.start()
	for range 30 {
		h.s.unconfirmWebhooks(h.now, "test")
		h.now = h.now.Add(time.Minute)
	}
	out := logs.String()
	if n := strings.Count(out, undeliveredMsg); n != 3 || !strings.Contains(out, "repeated=9") {
		t.Fatalf("%d warnings:\n%s", n, out)
	}
}

// Final review I-1: /status lags webhooks, and cloud copies usually arrive
// first. A lagging read is not a missed change.
func TestLaggingStatusAfterCloudEventIsNotAMissedChange(t *testing.T) {
	h, logs := loggedHarness(t)
	h.start()
	h.advance(10 * time.Second)
	h.send(cloudReached("")) // night via the cloud; the bridge copy lags
	h.run(time.Minute)       // the 1-minute read still shows day
	if h.bridge.statusCalls < 2 {
		t.Fatalf("status calls %d", h.bridge.statusCalls)
	}
	if strings.Contains(logs.String(), undeliveredMsg) || h.lock() != "LOCKED" {
		t.Fatalf("lock %s log %s", h.lock(), logs)
	}
}

// Final review M-2: a lock that ignores a command (no webhook on any feed)
// says nothing about bridge → gateway delivery.
func TestIgnoredLocalCommandDoesNotUnconfirm(t *testing.T) {
	h, logs := loggedHarness(t)
	h.start()
	h.confirmDelivery()
	h.advance(time.Second)
	h.cmd(model.CommandLock, "")
	h.run(40 * time.Second)
	if st := h.lastStatus(); st.Status != model.StatusFailed {
		t.Fatalf("status %+v", st)
	}
	if !h.s.webhookConfirmed || strings.Contains(logs.String(), undeliveredMsg) {
		t.Fatalf("confirmed %v log %s", h.s.webhookConfirmed, logs)
	}
}

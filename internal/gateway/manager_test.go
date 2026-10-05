package gateway

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/t3hk0d3/go-loqed/bridge"
	"github.com/t3hk0d3/go-loqed/cloud"
	"github.com/t3hk0d3/go-loqed/internal/config"
	"github.com/t3hk0d3/go-loqed/internal/model"
)

func TestManagerDispatch(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	m := NewManager([]*Supervisor{h.s})
	if err := m.DeliverBridgeEvent("nope", bridge.OnlineEvent{}); !errors.Is(err, ErrUnknownLock) {
		t.Fatalf("got %v", err)
	}
	if err := m.DeliverCloudEvent(cloud.WebhookEvent{LockID: "lock1"}); err != nil {
		t.Fatal(err)
	}
	if err := m.DeliverCommand("lock1", model.CommandLock, "", h.now); err != nil {
		t.Fatal(err)
	}
	for range 62 { // queue holds 64; 2 already queued
		_ = m.DeliverCommand("lock1", model.CommandLock, "", h.now)
	}
	if err := m.DeliverCommand("lock1", model.CommandLock, "", h.now); !errors.Is(err, ErrBusy) {
		t.Fatalf("got %v", err)
	}
	key, ok := m.BridgeKey("lock1")
	if !ok || string(key) != "bonjour monde" {
		t.Fatalf("key %q %v", key, ok)
	}
	if _, ok := m.Health()["lock1"]; !ok {
		t.Fatal("health missing lock1")
	}
}

func TestManagerRemoveStopsSupervisor(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.s.d.Now = time.Now // Run uses the real ticker
	m := NewManager([]*Supervisor{h.s})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { m.Run(ctx); close(done) }()
	deadline := time.Now().Add(5 * time.Second)
	for len(m.Health()) == 1 && m.Health()["lock1"].Mode != model.ModeLocal {
		if time.Now().After(deadline) {
			t.Fatal("supervisor did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := m.Remove([]string{"lock1", "nope"}); len(got) != 1 {
		t.Fatalf("removed %v", got)
	}
	if err := m.DeliverCommand("lock1", model.CommandOpen, "", time.Now()); !errors.Is(err, ErrUnknownLock) {
		t.Fatalf("got %v", err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run must return once every supervisor stopped")
	}
}

package gateway

import (
	"context"
	"log/slog"
	"testing"
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/cloud"
	"github.com/t3hk0d3/go-loqed/internal/config"
	"github.com/t3hk0d3/go-loqed/internal/model"
	"github.com/t3hk0d3/go-loqed/internal/store"
)

// lockAPI is a fake Lock API: commands move the bolt, reads report it.
type lockAPI struct {
	bolt  loqed.BoltState
	reads int
}

func (a *lockAPI) ListLocks(context.Context) ([]cloud.Lock, error) {
	a.reads++
	id := 1
	return []cloud.Lock{{ID: "lock1", BoltState: a.bolt, Online: model.Ptr(true), BridgeIP: "192.0.2.10",
		LocalID: &id, KeySecret: "SGFsbG8gd2VyZWxk", BridgeKey: "Ym9uam91ciBtb25kZQ=="}}, nil
}

func (a *lockAPI) Command(_ context.Context, _ string, s loqed.BoltState) error {
	a.bolt = s
	return nil
}

// realCloud wires the harness to a real CloudHub, Budget and Refresher.
func realCloud(t *testing.T, h *harness, api *lockAPI) (*CloudHub, *Refresher) {
	t.Helper()
	clock := func() time.Time { return h.now }
	hub := NewCloudHub(NewBudget(10, 12*time.Hour, clock, store.BudgetState{}, nil), &fakeTokens{token: "tok"},
		func(string) CloudAPI { return api }, clock, slog.New(slog.DiscardHandler))
	st := newStore(t)
	ref := NewRefresher(hub, st, clock)
	h.s.d.Cloud = hub
	h.s.d.Refresh = ref.Refresh
	return hub, ref
}

// UNLOCK in cloud mode: the confirmation poll 5 s later must not be
// answered with the lock list cached just before the command.
func TestRealHubConfirmShowsCommandResult(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	api := &lockAPI{bolt: loqed.BoltNightLock}
	realCloud(t, h, api)
	h.start()
	h.toCloud()
	if h.lock() != "LOCKED" {
		t.Fatalf("lock %s", h.lock())
	}
	h.run(10 * time.Second) // well inside the 30 s sharing window
	h.command(model.CommandUnlock)
	h.run(6 * time.Second)
	if h.lock() != "UNLOCKED" || h.state().StateStale {
		t.Fatalf("lock %s stale %v reads %d", h.lock(), h.state().StateStale, api.reads)
	}
}

// A bridge whose Wi-Fi flaps every few minutes for 12 h must not drain the
// shared budget: refresh backoff and the reserve leave room for confirms.
func TestRealHubFlappingBridgeKeepsBudget(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	api := &lockAPI{bolt: loqed.BoltNightLock}
	hub, _ := realCloud(t, h, api)
	h.start()
	for range 48 { // 12 h: 3 min down, 12 min up
		h.probeErr = loqed.ErrUnreachable
		for range 3 {
			h.advance(time.Minute)
		}
		h.probeErr = nil
		for range 12 {
			h.advance(time.Minute)
		}
	}
	if hub.Budget().Remaining() < 1 {
		t.Fatalf("budget drained by a flapping bridge: %d reads", api.reads)
	}
	if api.reads > 10 {
		t.Fatalf("%d reads in 12h exceed the budget", api.reads)
	}
}

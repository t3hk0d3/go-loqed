package gateway

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/cloud"
	"github.com/t3hk0d3/go-loqed/loqed-mqtt/internal/config"
	"github.com/t3hk0d3/go-loqed/loqed-mqtt/internal/model"
	"github.com/t3hk0d3/go-loqed/loqed-mqtt/internal/store"
)

func TestCloudReadInFlightStartsNoSecondRead(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.toCloud()
	before := len(h.cloud.calls)
	h.holdReads = true
	h.advance(h.s.t.CloudPoll) // background poll due: starts a read
	h.advance(h.s.t.CloudPoll) // due again while the first is unanswered
	if got := len(h.cloud.calls) - before; got != 1 {
		t.Fatalf("started %d reads while one was in flight, want 1", got)
	}
}

func TestQueuedCloudRequestsMerge(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.toCloud()
	ctx := t.Context()
	h.holdReads = true
	h.s.requestCloud(ctx, PriorityBackground, time.Time{})
	h.s.requestCloud(ctx, PriorityConfirm, h.now.Add(time.Second))
	h.s.requestCloud(ctx, PriorityConfirm, h.now.Add(2*time.Second))
	h.s.requestCloud(ctx, PriorityBackground, time.Time{})
	before := len(h.cloud.calls)
	h.releaseReads()
	if got := h.cloud.calls[before:]; len(got) != 1 || got[0] != PriorityConfirm {
		t.Fatalf("after the read: started %v, want one confirmation read", got)
	}
	if nb := h.cloud.notBefore[len(h.cloud.notBefore)-1]; !nb.Equal(h.now.Add(2 * time.Second)) {
		t.Fatalf("queued confirmation read notBefore = %v, want the later one", nb)
	}
}

func TestCommandIsSentWhileACloudReadIsOutstanding(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.toCloud()
	h.holdReads = true
	h.advance(h.s.t.CloudPoll)
	if h.s.read == nil {
		t.Fatal("no read in flight")
	}
	h.command(model.CommandLock)
	if len(h.cloud.commands) != 1 {
		t.Fatalf("cloud commands = %v, want the LOCK sent while the read is outstanding", h.cloud.commands)
	}
}

// backToLocal holds a background poll fetched now, then lets the bridge
// answer again so the lock returns to local mode with a newer /status.
func backToLocal(t *testing.T, h *harness, startRead func()) {
	t.Helper()
	h.holdReads = true
	startRead()
	h.probeErr = nil
	h.bridge.statusErr = nil
	h.bridge.status.BoltState = loqed.BoltDayLock
	h.cloud.locks[0].BoltState = loqed.BoltNightLock
	for range 3 {
		if h.s.mode == model.ModeLocal {
			break
		}
		h.advance(h.s.t.Liveness)
	}
	if h.s.mode != model.ModeLocal {
		t.Fatalf("expected local mode, got %s", h.s.mode)
	}
}

func TestBackgroundPollOlderThanStatusIsNotApplied(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.toCloud()
	backToLocal(t, h, func() { h.advance(h.s.t.CloudPoll) })
	h.releaseReads()
	if st := h.state(); st.BoltState != loqed.BoltDayLock || st.StateStale {
		t.Fatalf("poll fetched before the /status overwrote it: bolt %s stale %v", st.BoltState, st.StateStale)
	}
}

func TestConfirmationReadOlderThanStatusIsNotApplied(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.toCloud()
	backToLocal(t, h, func() {
		h.command(model.CommandLock) // via the cloud: schedules a confirmation read
		h.advance(h.s.t.CloudConfirm)
	})
	h.releaseReads()
	if st := h.state(); st.BoltState != loqed.BoltDayLock {
		t.Fatalf("confirmation read fetched before the /status overwrote it: %s", st.BoltState)
	}
}

// blockingCloud blocks every read until its context ends or release closes.
type blockingCloud struct {
	release  chan struct{}
	mu       sync.Mutex
	commands []loqed.BoltState
}

func (b *blockingCloud) Locks(ctx context.Context, _ Priority, _ time.Time) (LockList, error) {
	select {
	case <-ctx.Done():
		return LockList{}, ctx.Err()
	case <-b.release:
		return LockList{}, loqed.ErrNoResponse
	}
}

func (b *blockingCloud) Command(_ context.Context, _ string, s loqed.BoltState) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.commands = append(b.commands, s)
	return nil
}

func (b *blockingCloud) sent() int { b.mu.Lock(); defer b.mu.Unlock(); return len(b.commands) }

func TestRunHandlesCommandsWhileACloudReadBlocks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bc := &blockingCloud{release: make(chan struct{})}
		h := newHarness(t, store.LockRecord{ID: "lock1", Name: "Cloud only"}, config.LockSetting{}, func(d *Deps) {
			d.Cloud = bc
			d.Go = func(f func()) { go f() }
		})
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan struct{})
		go func() { h.s.Run(ctx); close(done) }() // cloud mode: its first read blocks
		synctest.Wait()
		if !h.s.Deliver(CommandMsg{Command: model.CommandLock, At: h.now}) {
			t.Fatal("command not accepted")
		}
		synctest.Wait()
		if bc.sent() != 1 {
			t.Fatal("the command waited for the blocked cloud read")
		}
		cancel()
		<-done
		synctest.Wait() // the read goroutine must have ended too
	})
}

type gatedAPI struct {
	gate     chan struct{}
	mu       sync.Mutex
	commands int
}

func (a *gatedAPI) ListLocks(ctx context.Context) ([]cloud.Lock, error) {
	select {
	case <-a.gate:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return []cloud.Lock{{ID: "lock1"}, {ID: "lock2"}}, nil
}

func (a *gatedAPI) Command(context.Context, string, loqed.BoltState) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.commands++
	return nil
}

func TestOneLocksBlockedReadDoesNotDelayAnotherLocksCommand(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		api := &gatedAPI{gate: make(chan struct{})}
		hub := NewCloudHub(NewBudget(10, 12*time.Hour, time.Now, store.BudgetState{}, nil), &fakeTokens{token: "tok"},
			func(string) CloudAPI { return api }, time.Now, slog.New(slog.DiscardHandler))
		useHub := func(d *Deps) {
			d.Cloud = hub
			d.Go = func(f func()) { go f() }
		}
		a := newHarness(t, store.LockRecord{ID: "lock1", Name: "A"}, config.LockSetting{}, useHub)
		b := newHarness(t, store.LockRecord{ID: "lock2", Name: "B"}, config.LockSetting{}, useHub)
		a.start() // cloud only: A's read holds the hub while ListLocks waits
		b.start() // B's read waits for the hub
		synctest.Wait()
		b.command(model.CommandLock)
		api.mu.Lock()
		n := api.commands
		api.mu.Unlock()
		if n != 1 {
			t.Fatal("lock B's command waited for lock A's cloud read")
		}
		close(api.gate)
		synctest.Wait()
	})
}

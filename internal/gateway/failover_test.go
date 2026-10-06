package gateway

import (
	"context"
	"slices"
	"testing"
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/internal/config"
	"github.com/t3hk0d3/go-loqed/internal/model"
	"github.com/t3hk0d3/go-loqed/internal/store"
)

func TestThreeFailuresSwitchToCloud(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.probeErr = loqed.ErrUnreachable
	h.advance(60 * time.Second)
	h.advance(60 * time.Second)
	if h.s.mode != model.ModeLocal {
		t.Fatal("two failures must not switch modes")
	}
	h.advance(60 * time.Second)
	if h.s.mode != model.ModeCloud || len(h.refreshes) != 1 || h.refreshes[0] != ReasonUnreachable {
		t.Fatalf("mode %s refreshes %v", h.s.mode, h.refreshes)
	}
	if len(h.cloud.calls) != 1 || h.cloud.calls[0] != PriorityBackground {
		t.Fatalf("cloud calls %v", h.cloud.calls)
	}
	if h.lock() != "LOCKED" || h.state().Mode != model.ModeCloud || !h.available() || h.state().StateStale {
		t.Fatalf("lock %s state %+v", h.lock(), h.state())
	}
}

func TestIPChangeReconnectsLocally(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	moved := testRecord()
	moved.BridgeIP = "192.0.2.77"
	h.refreshRec = &moved
	h.probeErr = loqed.ErrUnreachable
	for range 3 {
		h.advance(60 * time.Second)
	}
	if h.s.mode != model.ModeLocal || h.s.Record().BridgeIP != "192.0.2.77" || h.bridgesBuilt != 2 {
		t.Fatalf("mode %s ip %s bridges %d", h.s.mode, h.s.Record().BridgeIP, h.bridgesBuilt)
	}
}

func TestPinnedBridgeIPSkipsRefresh(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{BridgeIP: "192.0.2.99"})
	h.start()
	h.toCloud()
	if len(h.refreshes) != 0 || h.probes[0] != "192.0.2.99:80" {
		t.Fatalf("refreshes %v probes %v", h.refreshes, h.probes)
	}
}

func TestCloudModeReturnsToLocal(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.toCloud()
	h.probeErr = nil
	h.advance(60 * time.Second)
	if h.s.mode != model.ModeLocal || h.lock() != "UNLOCKED" {
		t.Fatalf("mode %s lock %s", h.s.mode, h.lock())
	}
}

// Offline detection uses an unbudgeted TCP probe of the cloud host, so it
// takes minutes, not hours of deferred polls.
func TestCloudProbeFailuresGoOfflineAndRecoverWithinFiveMinutes(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.toCloud()
	h.cloudProbeErr = loqed.ErrUnreachable
	for range 3 {
		h.advance(time.Minute)
	}
	if h.s.mode != model.ModeOffline || h.available() || h.state().Mode != model.ModeOffline {
		t.Fatalf("mode %s", h.s.mode)
	}
	h.cloudProbeErr = nil
	h.advance(4 * time.Minute)
	if h.s.mode != model.ModeOffline {
		t.Fatal("must wait 5 minutes before retrying")
	}
	h.advance(time.Minute)
	if h.s.mode != model.ModeCloud {
		t.Fatalf("mode %s", h.s.mode)
	}
}

func TestCloudAPIFailuresGoOffline(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.toCloud()
	h.cloud.err = loqed.ErrNoResponse
	for range 3 {
		h.advance(time.Minute)
	}
	if h.s.mode != model.ModeOffline {
		t.Fatalf("mode %s", h.s.mode)
	}
}

func TestOfflineRetriesForeverWithoutSpendingBudget(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.toCloud()
	h.cloudProbeErr = loqed.ErrUnreachable
	for range 3 {
		h.advance(time.Minute)
	}
	before, calls, cloudBefore := len(h.probes), len(h.cloud.calls), h.cloudProbes
	for range 24 { // two hours
		h.advance(5 * time.Minute)
	}
	if h.s.mode != model.ModeOffline || len(h.probes)-before != 24 || len(h.cloud.calls) != calls || h.cloudProbes-cloudBefore != 24 {
		t.Fatalf("mode %s probes %d cloud calls %d", h.s.mode, len(h.probes)-before, len(h.cloud.calls)-calls)
	}
}

func TestBudgetExhaustionMarksStateStale(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.toCloud()
	h.cloud.err = ErrBudgetExhausted
	h.advance(time.Minute)
	if !h.state().StateStale || h.s.mode != model.ModeCloud {
		t.Fatalf("stale %v mode %s", h.state().StateStale, h.s.mode)
	}
	h.cloud.err = nil
	h.cloud.locks[0].BoltState = loqed.BoltDayLock
	h.advance(time.Minute)
	if h.state().StateStale || h.lock() != "UNLOCKED" {
		t.Fatalf("fresh poll must clear stale: %+v", h.state())
	}
}

func TestEnteringCloudIsStaleUntilFreshData(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.cloud.err = ErrDeferred
	h.toCloud()
	if !h.state().StateStale || h.lock() != "UNLOCKED" {
		t.Fatalf("last local state must be marked stale in cloud mode: %+v", h.state())
	}
}

func TestDeferredPollsEventuallyMarkStale(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.toCloud() // fresh poll
	h.cloud.err = ErrDeferred
	h.advance(80 * time.Minute)
	if h.state().StateStale {
		t.Fatal("within spacing + grace")
	}
	h.advance(5 * time.Minute)
	if !h.state().StateStale {
		t.Fatal("no fresh data for longer than spacing + grace must be stale")
	}
}

func TestPollOlderThanEventIsIgnored(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.toCloud()
	h.bridge.statusErr = loqed.ErrNoResponse    // stay in cloud mode
	h.send(reached("STATE_CHANGED_LATCH", nil)) // event at now
	h.cloud.fetchedAt = h.now.Add(-time.Second) // poll data from before it
	h.cloud.locks[0].BoltState = loqed.BoltNightLock
	h.s.pollCloud(t.Context(), PriorityConfirm, time.Time{})
	if h.lock() != "UNLOCKED" {
		t.Fatalf("older poll overwrote newer event: %s", h.lock())
	}
}

func TestMissingOnlineKeepsPreviousValue(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.toCloud()
	h.cloud.locks[0].Online = model.Ptr(false)
	h.s.pollCloud(t.Context(), PriorityConfirm, time.Time{})
	h.cloud.locks[0].Online = nil
	h.s.pollCloud(t.Context(), PriorityConfirm, time.Time{})
	if h.state().LockOnline {
		t.Fatal("absent online must keep the previous value")
	}
}

func TestCloudOnlyLockStartsInCloud(t *testing.T) {
	h := newHarness(t, store.LockRecord{ID: "lock1", Name: "Pure"}, config.LockSetting{})
	h.start()
	if h.s.mode != model.ModeCloud || h.bridgesBuilt != 0 {
		t.Fatalf("mode %s bridges %d", h.s.mode, h.bridgesBuilt)
	}
	h.advance(60 * time.Second)
	if len(h.probes) != 0 {
		t.Fatal("cloud-only locks are never probed")
	}
}

func TestPollingResumesWhenCloudWebhooksStop(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{}, func(d *Deps) { d.CloudWebhooks = true })
	h.start()
	h.toCloud()
	h.s.lastCloudEventAt = h.now // a cloud webhook arrived
	h.advance(2 * time.Minute)   // push active: next poll is a reconcile away
	h.advance(time.Hour)
	h.s.lastCloudEventAt = h.now // the last one, an hour later
	lapse := h.s.lastCloudEventAt.Add(h.s.t.Reconcile)
	for h.now.Before(lapse) {
		h.advance(time.Minute)
	}
	calls := len(h.cloud.calls)
	h.advance(2 * time.Minute)
	if len(h.cloud.calls) == calls {
		t.Fatal("no poll within CloudPoll after cloud webhooks stopped")
	}
}

// commandFailoverHarness: the bridge is unreachable and two HTTP failures
// are already counted, so the next command's local failure is the third.
// It records what had happened when the credential refresh ran.
func commandFailoverHarness(t *testing.T) (h *harness, cloudCmdsAtRefresh *[]int, modeAtRefresh *[]model.Mode) {
	cmds, modes := &[]int{}, &[]model.Mode{}
	h = newHarness(t, testRecord(), config.LockSetting{}, func(d *Deps) {
		orig := d.Refresh
		d.Refresh = func(ctx context.Context, id string, r Reason) (store.LockRecord, error) {
			*cmds = append(*cmds, len(h.cloud.commands))
			*modes = append(*modes, h.s.mode)
			return orig(ctx, id, r)
		}
	})
	h.start()
	h.bridge.commandErrs = unreachable(1000)
	h.s.httpFailures = 2
	return h, cmds, modes
}

func TestCommandFailoverRunsAfterTheCloudAttempt(t *testing.T) {
	h, cmdsAt, modesAt := commandFailoverHarness(t)
	h.cmd(model.CommandLock, "")
	h.step(21 * time.Second) // past the 20 s local cutoff: the cloud command is sent
	if len(h.cloud.commands) != 1 {
		t.Fatalf("cloud commands %v", h.cloud.commands)
	}
	if len(h.refreshes) != 0 || h.s.mode != model.ModeLocal {
		t.Fatalf("failover before the command resolved: refreshes %v mode %s", h.refreshes, h.s.mode)
	}
	h.run(10 * time.Second) // the cloud confirmation poll confirms it
	if st := h.lastStatus(); st.Status != model.StatusConfirmed {
		t.Fatalf("status %+v", st)
	}
	if !slices.Equal(h.refreshes, []Reason{ReasonUnreachable}) || h.s.mode != model.ModeCloud {
		t.Fatalf("refreshes %v mode %s", h.refreshes, h.s.mode)
	}
	if !slices.Equal(*cmdsAt, []int{1}) || !slices.Equal(*modesAt, []model.Mode{model.ModeLocal}) {
		t.Fatalf("at refresh: cloud commands %v modes %v", *cmdsAt, *modesAt)
	}
}

func TestCommandBelowThresholdDoesNotFailOver(t *testing.T) {
	h, _, _ := commandFailoverHarness(t)
	h.s.httpFailures = 0
	h.cmd(model.CommandLock, "")
	h.step(31 * time.Second)
	if len(h.cloud.commands) != 1 || len(h.refreshes) != 0 || h.s.mode != model.ModeLocal {
		t.Fatalf("cloud %v refreshes %v mode %s", h.cloud.commands, h.refreshes, h.s.mode)
	}
}

func TestCommandFailoverSkippedWhenAlreadyLeftLocal(t *testing.T) {
	h, _, _ := commandFailoverHarness(t)
	h.cmd(model.CommandLock, "")
	h.step(21 * time.Second)
	h.s.enterCloud(context.Background()) // something else failed over meanwhile
	h.run(40 * time.Second)              // the command resolves (confirmed or no_confirmation)
	if st := h.lastStatus(); st.Status != model.StatusConfirmed && st.Status != model.StatusFailed {
		t.Fatalf("status %+v", st)
	}
	if len(h.refreshes) != 0 || h.s.mode != model.ModeCloud {
		t.Fatalf("refreshes %v mode %s", h.refreshes, h.s.mode)
	}
}

func TestQueuedCommandAfterCommandFailoverUsesCloud(t *testing.T) {
	h, _, _ := commandFailoverHarness(t)
	h.cmd(model.CommandLock, "")
	h.step(21 * time.Second)
	bridgeCalls := len(h.bridge.commands)
	h.cmd(model.CommandUnlock, "") // waits for the in-flight LOCK
	h.run(10 * time.Second)
	if h.s.mode != model.ModeCloud {
		t.Fatalf("mode %s", h.s.mode)
	}
	if len(h.bridge.commands) != bridgeCalls {
		t.Fatalf("the queued command retried the bridge: %v", h.bridge.commands[bridgeCalls:])
	}
	if !slices.Equal(h.cloud.commands, []loqed.BoltState{loqed.BoltNightLock, loqed.BoltDayLock}) {
		t.Fatalf("cloud commands %v", h.cloud.commands)
	}
}

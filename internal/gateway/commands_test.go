package gateway

import (
	"slices"
	"testing"
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/bridge"
	"github.com/t3hk0d3/go-loqed/internal/config"
	"github.com/t3hk0d3/go-loqed/internal/model"
)

func TestLocalCommandUsesBridge(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.command(model.CommandLock)
	if len(h.bridge.commands) != 1 || h.bridge.commands[0] != bridge.ActionLock || len(h.cloud.commands) != 0 {
		t.Fatalf("bridge %v cloud %v", h.bridge.commands, h.cloud.commands)
	}
	h.command(model.CommandOpen)
	h.command(model.CommandUnlock)
	if h.bridge.commands[1] != bridge.ActionOpen || h.bridge.commands[2] != bridge.ActionUnlock {
		t.Fatalf("bridge %v", h.bridge.commands)
	}
}

func TestStaleCommandIsDroppedAndReported(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.send(CommandMsg{Command: model.CommandOpen, At: h.now.Add(-11 * time.Second)})
	if len(h.bridge.commands) != 0 || len(h.cloud.commands) != 0 {
		t.Fatal("a stale OPEN must never fire")
	}
	if got := h.failedCommands(); !slices.Equal(got, []string{model.FailExpired}) {
		t.Fatalf("command_failed %v", got)
	}
	ev := h.pub.events[0]
	if ev.Reason != "OPEN" || ev.Source != model.SourceGateway {
		t.Fatalf("event %+v", ev)
	}
}

// The bridge provably did not get the command: send it via the cloud, in
// the same request, within the command's deadline.
func TestUndeliveredCommandFallsBackToCloud(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.bridge.commandErrs = []error{loqed.ErrUnreachable}
	h.command(model.CommandLock)
	if len(h.cloud.commands) != 1 || h.cloud.commands[0] != loqed.BoltNightLock || h.lock() != "LOCKING" {
		t.Fatalf("cloud %v lock %s", h.cloud.commands, h.lock())
	}
	if d := h.cloud.cmdBudgets[0]; d > 10*time.Second || d < 9*time.Second {
		t.Fatalf("cloud command must run within the 10s command deadline: %v", d)
	}
	if h.s.httpFailures != 1 || len(h.refreshes) != 0 {
		t.Fatalf("failure counted after the command, no refresh yet: %d %v", h.s.httpFailures, h.refreshes)
	}
}

// The bridge may have acted (timeout after sending): never resend via the
// cloud (that would unlatch the door twice); verify and report instead.
func TestCommandWithoutResponseIsNeverResent(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.bridge.commandErrs = []error{loqed.ErrNoResponse}
	h.command(model.CommandOpen)
	if len(h.cloud.commands) != 0 || len(h.bridge.commands) != 1 {
		t.Fatalf("resent: bridge %v cloud %v", h.bridge.commands, h.cloud.commands)
	}
	if got := h.failedCommands(); !slices.Equal(got, []string{model.FailNoResponse}) {
		t.Fatalf("command_failed %v", got)
	}
	h.bridge.status.BoltState = loqed.BoltOpen
	h.advance(time.Second)
	if h.bridge.statusCalls != 2 || h.lock() != "OPEN" {
		t.Fatalf("immediate status expected: %d %s", h.bridge.statusCalls, h.lock())
	}
}

func TestCommandWithoutResponseFallsBackToCloudConfirm(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.bridge.commandErrs = []error{loqed.ErrNoResponse}
	h.command(model.CommandLock)
	h.bridge.statusErr = loqed.ErrNoResponse
	h.advance(time.Second) // status fails
	calls := len(h.cloud.calls)
	h.run(6 * time.Second)
	if len(h.cloud.calls) != calls+1 || h.cloud.calls[calls] != PriorityConfirm || len(h.cloud.commands) != 0 {
		t.Fatalf("calls %v commands %v", h.cloud.calls, h.cloud.commands)
	}
}

func TestCommandAuthErrorUsesCloudThenRefreshes(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.bridge.commandErrs = []error{loqed.ErrUnauthorized}
	h.command(model.CommandUnlock)
	if len(h.bridge.commands) != 1 || len(h.cloud.commands) != 1 || len(h.refreshes) != 1 || h.refreshes[0] != ReasonUnauthorized {
		t.Fatalf("bridge %v cloud %v refreshes %v", h.bridge.commands, h.cloud.commands, h.refreshes)
	}
}

// A command that cannot reach the bridge until its deadline is never sent
// late through the cloud.
func TestCommandPastDeadlineIsNotSentViaCloud(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.bridge.commandErrs = []error{loqed.ErrUnreachable}
	h.send(CommandMsg{Command: model.CommandOpen, At: h.now.Add(-9 * time.Second)})
	if len(h.cloud.commands) != 1 || h.cloud.cmdBudgets[0] > time.Second {
		t.Fatalf("the cloud call must carry only the remaining 1s: %v", h.cloud.cmdBudgets)
	}
}

func TestMissedWebhookTriggersStatusAfterTenSeconds(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.command(model.CommandLock)
	h.run(9 * time.Second)
	if h.bridge.statusCalls != 1 {
		t.Fatal("too early")
	}
	h.run(time.Second)
	if h.bridge.statusCalls != 2 {
		t.Fatalf("status calls %d", h.bridge.statusCalls)
	}
}

func TestWebhookConfirmationCancelsStatus(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.command(model.CommandLock)
	h.send(reached("STATE_CHANGED_NIGHT_LOCK", nil))
	h.run(15 * time.Second)
	if h.bridge.statusCalls != 1 {
		t.Fatalf("status calls %d", h.bridge.statusCalls)
	}
}

func TestOfflineRejectsCommands(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.toCloud()
	h.cloudProbeErr = loqed.ErrUnreachable
	for range 3 {
		h.advance(time.Minute)
	}
	h.command(model.CommandOpen)
	if len(h.cloud.commands) != 0 || len(h.bridge.commands) != 0 {
		t.Fatal("offline must reject commands")
	}
	if got := h.failedCommands(); !slices.Equal(got, []string{model.FailOffline}) {
		t.Fatalf("command_failed %v", got)
	}
}

func TestCloudModeCommandConfirmPollIgnoresOlderData(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.toCloud()
	calls := len(h.cloud.calls)
	at := h.now
	h.command(model.CommandUnlock)
	if len(h.cloud.commands) != 1 || h.cloud.commands[0] != loqed.BoltDayLock || h.lock() != "UNLOCKING" {
		t.Fatalf("cloud %v lock %s", h.cloud.commands, h.lock())
	}
	h.run(5 * time.Second)
	if len(h.cloud.calls) != calls+1 || h.cloud.calls[calls] != PriorityConfirm || !h.cloud.notBefore[calls].Equal(at) {
		t.Fatalf("calls %v notBefore %v", h.cloud.calls, h.cloud.notBefore)
	}
}

// After a local→cloud fallback the confirmation must come from the cloud;
// the bridge is down, so a /status confirm would leave UNLOCKING for an hour.
func TestFallbackCommandIsConfirmedViaCloud(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.bridge.commandErrs = []error{loqed.ErrUnreachable}
	h.command(model.CommandUnlock)
	h.cloud.locks[0].BoltState = loqed.BoltDayLock
	h.run(5 * time.Second)
	if h.lock() != "UNLOCKED" || h.cloud.calls[len(h.cloud.calls)-1] != PriorityConfirm {
		t.Fatalf("lock %s calls %v", h.lock(), h.cloud.calls)
	}
}

// A cloud command whose response timed out may have run: confirm anyway.
func TestCloudCommandTimeoutStillConfirms(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.toCloud()
	h.cloud.commandErr = loqed.ErrNoResponse
	calls := len(h.cloud.calls)
	h.command(model.CommandLock)
	h.run(5 * time.Second)
	if len(h.cloud.calls) != calls+1 || h.cloud.calls[calls] != PriorityConfirm {
		t.Fatalf("calls %v", h.cloud.calls)
	}
	if got := h.failedCommands(); !slices.Equal(got, []string{model.FailNoResponse}) {
		t.Fatalf("command_failed %v", got)
	}
}

// A refresh that returns unusable credentials must not leave a nil bridge
// client in local mode (that panicked and crash-looped the process).
func TestCommandWithUnusableRefreshUsesCloudWithoutPanic(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	broken := testRecord()
	broken.LocalID = nil // the cloud stopped reporting local credentials
	h.refreshRec = &broken
	h.bridge.commandErrs = []error{loqed.ErrUnauthorized}
	h.command(model.CommandLock)
	if h.s.mode != model.ModeCloud || h.s.bridge != nil {
		t.Fatalf("mode %s bridge %v", h.s.mode, h.s.bridge)
	}
	h.run(30 * time.Second) // ticks must not touch the nil bridge
	if len(h.cloud.commands) != 1 {
		t.Fatalf("the command must still go out via the cloud: %v", h.cloud.commands)
	}
}

// The failure that trips failover must not lose the confirmation of a
// command that may have run.
func TestUncertainCommandOnFailoverIsStillConfirmed(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.s.httpFailures = 2
	h.bridge.commandErrs = []error{loqed.ErrNoResponse}
	h.command(model.CommandOpen)
	if h.s.mode != model.ModeCloud || len(h.cloud.commands) != 0 {
		t.Fatalf("mode %s cloud commands %v", h.s.mode, h.cloud.commands)
	}
	calls := len(h.cloud.calls)
	h.run(6 * time.Second)
	if !slices.Contains(h.cloud.calls[calls:], PriorityConfirm) {
		t.Fatalf("no confirm poll: %v", h.cloud.calls[calls:])
	}
}

func TestCommandExpiringDuringBridgeAttemptIsNotSentViaCloud(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.bridge.commandErrs = []error{loqed.ErrUnreachable}
	h.bridge.onCommand = func() { time.Sleep(100 * time.Millisecond) }
	h.send(CommandMsg{Command: model.CommandOpen, At: h.now.Add(-10*time.Second + 50*time.Millisecond)})
	if len(h.cloud.commands) != 0 {
		t.Fatalf("late cloud send: %v", h.cloud.commands)
	}
	if got := h.failedCommands(); !slices.Equal(got, []string{model.FailExpired}) {
		t.Fatalf("command_failed %v", got)
	}
}

// The cloud may still report the pre-command state at the first confirmation
// poll; that must not be published as fresh. One more poll catches up.
func TestLaggingCloudConfirmationIsStaleThenRetriedOnce(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.cloud.locks[0].BoltState = loqed.BoltDayLock
	h.toCloud()
	confirms := func() (n int) {
		for _, p := range h.cloud.calls {
			if p == PriorityConfirm {
				n++
			}
		}
		return n
	}
	h.command(model.CommandLock)
	h.run(5 * time.Second)
	if confirms() != 1 || h.lock() != "UNLOCKED" || !h.state().StateStale {
		t.Fatalf("lagging poll: confirms %d lock %s stale %v", confirms(), h.lock(), h.state().StateStale)
	}
	h.cloud.locks[0].BoltState = loqed.BoltNightLock
	h.run(5 * time.Second)
	if confirms() != 2 || h.lock() != "LOCKED" || h.state().StateStale {
		t.Fatalf("retry: confirms %d lock %s stale %v", confirms(), h.lock(), h.state().StateStale)
	}
	h.run(30 * time.Second)
	if confirms() != 2 {
		t.Fatalf("confirms %d", confirms())
	}
}

// After the one retry a still-lagging cloud stays stale; no third poll.
func TestLaggingCloudConfirmationRetriesAtMostOnce(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.cloud.locks[0].BoltState = loqed.BoltDayLock
	h.toCloud()
	h.command(model.CommandLock)
	h.run(30 * time.Second)
	n := 0
	for _, p := range h.cloud.calls {
		if p == PriorityConfirm {
			n++
		}
	}
	if n != 2 || !h.state().StateStale {
		t.Fatalf("confirms %d stale %v", n, h.state().StateStale)
	}
}

// A background poll started by the failover that follows a cloud-sent command
// may carry data fetched before the command; it must not undo LOCKING.
func TestFallbackPollWithPreCommandDataKeepsMovingState(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.s.httpFailures = 2
	h.bridge.commandErrs = []error{loqed.ErrUnreachable}
	h.cloud.locks[0].BoltState = loqed.BoltDayLock
	h.cloud.fetchedAt = h.now.Add(-5 * time.Second)
	h.command(model.CommandLock)
	if h.s.mode != model.ModeCloud || h.lock() != "LOCKING" {
		t.Fatalf("mode %s lock %s", h.s.mode, h.lock())
	}
	h.cloud.fetchedAt = time.Time{}
	h.cloud.locks[0].BoltState = loqed.BoltNightLock
	h.run(5 * time.Second)
	if h.lock() != "LOCKED" {
		t.Fatalf("lock %s", h.lock())
	}
}

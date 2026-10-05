package gateway

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/bridge"
	"github.com/t3hk0d3/go-loqed/cloud"
	"github.com/t3hk0d3/go-loqed/internal/config"
	"github.com/t3hk0d3/go-loqed/internal/model"
)

// ourKey is testRecord's local_id: the gateway's own key.
var ourKey = model.Ptr(1)

func (h *harness) cmd(c model.Command, id string) { h.send(CommandMsg{Command: c, ID: id, At: h.now}) }

// trail renders the published command statuses as "status/error@id".
func (h *harness) trail() []string {
	var out []string
	for _, s := range h.pub.statuses {
		e := ""
		if s.Error != nil {
			e = "/" + *s.Error
		}
		id := ""
		if s.ID != nil {
			id = "@" + *s.ID
		}
		out = append(out, string(s.Status)+e+id)
	}
	return out
}

func (h *harness) lastStatus() model.CommandStatus {
	h.t.Helper()
	if len(h.pub.statuses) == 0 {
		h.t.Fatal("no command status published")
	}
	return h.pub.statuses[len(h.pub.statuses)-1]
}

func (h *harness) wantTrail(want ...string) {
	h.t.Helper()
	if got := h.trail(); !slices.Equal(got, want) {
		h.t.Fatalf("command statuses\n got  %v\n want %v", got, want)
	}
}

// step advances the clock in small steps so sub-second retries run.
func (h *harness) step(d time.Duration) {
	for end := h.now.Add(d); h.now.Before(end); {
		h.advance(250 * time.Millisecond)
	}
}

func unreachable(n int) []error {
	errs := make([]error, n)
	for i := range errs {
		errs[i] = fmt.Errorf("%w: connection refused", loqed.ErrUnreachable)
	}
	return errs
}

func ourGoTo(target loqed.BoltState) BridgeEventMsg {
	return BridgeEventMsg{Event: bridge.GoToStateEvent{EventType: "GO_TO_STATE_MANUAL_LOCK_REMOTE_" + strings.ToUpper(string(target)),
		GoToState: target, KeyLocalID: ourKey}}
}

// --- Submit ---

func TestSubmitPublishesPendingAndDeliversAtOnce(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	at := h.now
	h.cmd(model.CommandLock, "a1")
	h.wantTrail("pending@a1", "sending@a1", "sent@a1")
	st := h.lastStatus()
	if *st.Via != model.ViaLocal || st.Attempts != 1 || !st.ReceivedAt.Equal(at) || st.Command != model.CommandLock {
		t.Fatalf("status %+v", st)
	}
	if !slices.Equal(h.bridge.commands, []bridge.Action{bridge.ActionLock}) {
		t.Fatalf("bridge %v", h.bridge.commands)
	}
}

func TestSubmitReplacesCommandNotYetWritten(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.bridge.commandErrs = unreachable(1)
	h.cmd(model.CommandUnlock, "old")
	h.advance(200 * time.Millisecond)
	h.cmd(model.CommandLock, "new")
	h.wantTrail("pending@old", "sending@old", "superseded@old", "pending@new", "sending@new", "sent@new")
	if !slices.Equal(h.bridge.commands, []bridge.Action{bridge.ActionUnlock, bridge.ActionLock}) {
		t.Fatalf("bridge %v", h.bridge.commands)
	}
	h.step(5 * time.Second)
	if len(h.bridge.commands) != 2 {
		t.Fatalf("the superseded command must never be retried: %v", h.bridge.commands)
	}
}

func TestSubmitCoalescesEqualInFlightCommand(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.cmd(model.CommandLock, "a")
	h.cmd(model.CommandLock, "b")
	if len(h.bridge.commands) != 1 {
		t.Fatalf("equal command sent again: %v", h.bridge.commands)
	}
	h.wantTrail("pending@a", "sending@a", "sent@a", "sent@b")
	h.cmd(model.CommandLock, "")
	if st := h.lastStatus(); *st.ID != "b" {
		t.Fatalf("an empty id keeps the current one: %+v", st)
	}
}

func TestSubmitCoalescesEqualPendingCommand(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.cmd(model.CommandUnlock, "a")
	h.cmd(model.CommandLock, "b")
	h.cmd(model.CommandLock, "c")
	h.send(reached("STATE_CHANGED_LATCH", ourKey))
	if !slices.Equal(h.bridge.commands, []bridge.Action{bridge.ActionUnlock, bridge.ActionLock}) {
		t.Fatalf("bridge %v", h.bridge.commands)
	}
	if st := h.lastStatus(); *st.ID != "c" || st.Status != model.StatusSent {
		t.Fatalf("status %+v", st)
	}
}

func TestSubmitWaitsForTheInFlightCommand(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start() // day_lock
	h.cmd(model.CommandLock, "a")
	h.cmd(model.CommandUnlock, "b")
	if len(h.bridge.commands) != 1 {
		t.Fatalf("a written command cannot be recalled; the new one waits: %v", h.bridge.commands)
	}
	h.wantTrail("pending@a", "sending@a", "sent@a", "pending@b")
	h.send(reached("STATE_CHANGED_NIGHT_LOCK", ourKey))
	if !slices.Equal(h.bridge.commands, []bridge.Action{bridge.ActionLock, bridge.ActionUnlock}) {
		t.Fatalf("the pending command must start once the first resolves: %v", h.bridge.commands)
	}
	h.wantTrail("pending@a", "sending@a", "sent@a", "pending@b", "confirmed@a", "sending@b", "sent@b")
}

func TestLatestCommandReplacesQueuedOne(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.cmd(model.CommandLock, "a")
	h.cmd(model.CommandUnlock, "b")
	h.cmd(model.CommandOpen, "c")
	h.wantTrail("pending@a", "sending@a", "sent@a", "pending@b", "superseded@b", "pending@c")
	h.send(reached("STATE_CHANGED_NIGHT_LOCK", ourKey))
	if !slices.Equal(h.bridge.commands, []bridge.Action{bridge.ActionLock, bridge.ActionOpen}) {
		t.Fatalf("bridge %v", h.bridge.commands)
	}
}

func TestInFlightCommandIsNeverRetried(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.bridge.statusErr = loqed.ErrNoResponse // confirmation reads fail too
	h.cmd(model.CommandLock, "a")
	h.run(2 * time.Minute)
	if len(h.bridge.commands) != 1 || len(h.cloud.commands) != 0 {
		t.Fatalf("resent: bridge %v cloud %v", h.bridge.commands, h.cloud.commands)
	}
	if st := h.lastStatus(); st.Status != model.StatusFailed || *st.Error != model.FailNoConfirmation {
		t.Fatalf("status %+v", st)
	}
}

func TestOfflineLockFailsCommandsAtOnce(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.toCloud()
	h.cloudProbeErr = loqed.ErrUnreachable
	for range 3 {
		h.advance(time.Minute)
	}
	h.cmd(model.CommandOpen, "x")
	if len(h.cloud.commands) != 0 || len(h.bridge.commands) != 0 {
		t.Fatal("offline must reject commands")
	}
	h.wantTrail("failed/offline@x")
	if got := h.failedCommands(); !slices.Equal(got, []string{model.FailOffline}) {
		t.Fatalf("command_failed %v", got)
	}
	if ev := h.pub.events[len(h.pub.events)-1]; ev.Reason != "OPEN" || ev.Source != model.SourceGateway {
		t.Fatalf("event %+v", ev)
	}
}

// --- deliverLocal ---

func TestBridgeAckIsOnlySent(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start() // day_lock
	h.cmd(model.CommandLock, "")
	if h.lastStatus().Status != model.StatusSent || h.lock() != "LOCKING" {
		t.Fatalf("status %+v lock %s", h.lastStatus(), h.lock())
	}
	h.run(29 * time.Second)
	if h.bridge.statusCalls != 1 || h.lastStatus().Status != model.StatusSent {
		t.Fatalf("too early: status %d %+v", h.bridge.statusCalls, h.lastStatus())
	}
	h.run(time.Second)
	if h.bridge.statusCalls != 2 {
		t.Fatalf("the confirmation window is 30 s: %d", h.bridge.statusCalls)
	}
}

func TestCommandForCurrentStateShowsNoMovement(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start() // day_lock
	h.cmd(model.CommandUnlock, "")
	if h.lock() != "UNLOCKED" {
		t.Fatalf("lock %s", h.lock())
	}
}

func TestUnreachableBridgeIsRetriedWithBackoffThenCloud(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	start := h.now
	var at []time.Duration
	h.bridge.onCommand = func() { at = append(at, h.now.Sub(start)) }
	h.bridge.commandErrs = unreachable(100)
	h.cmd(model.CommandLock, "")
	h.step(20 * time.Second)
	want := []time.Duration{0, 500 * time.Millisecond, 1500 * time.Millisecond}
	for t2 := 3500 * time.Millisecond; t2 < 20*time.Second; t2 += 2 * time.Second {
		want = append(want, t2)
	}
	if !slices.Equal(at, want) {
		t.Fatalf("local attempts at\n got  %v\n want %v", at, want)
	}
	if len(h.cloud.commands) != 1 || h.cloud.commands[0] != loqed.BoltNightLock {
		t.Fatalf("one cloud command after the local cutoff: %v", h.cloud.commands)
	}
	if d := h.cloud.cmdBudgets[0]; d > 10*time.Second+time.Second || d < 9*time.Second {
		t.Fatalf("the cloud call runs within the command deadline: %v", d)
	}
	st := h.lastStatus()
	if st.Status != model.StatusSent || *st.Via != model.ViaCloud || st.Attempts != len(want)+1 {
		t.Fatalf("status %+v", st)
	}
	if h.s.httpFailures != 1 {
		t.Fatalf("one HTTP failure per command, not per attempt: %d", h.s.httpFailures)
	}
}

func TestOpenStopsLocalRetriesThreeSecondsBeforeDeadline(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	start := h.now
	var at []time.Duration
	h.bridge.onCommand = func() { at = append(at, h.now.Sub(start)) }
	h.bridge.commandErrs = unreachable(100)
	h.cmd(model.CommandOpen, "")
	h.step(12 * time.Second)
	want := []time.Duration{0, 500 * time.Millisecond, 1500 * time.Millisecond, 3500 * time.Millisecond, 5500 * time.Millisecond}
	if !slices.Equal(at, want) || len(h.cloud.commands) != 1 || h.cloud.commands[0] != loqed.BoltOpen {
		t.Fatalf("local %v cloud %v", at, h.cloud.commands)
	}
}

func TestUnreachableWithoutCloudAccessFails(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.bridge.commandErrs = unreachable(100)
	h.cloud.commandErr = fmt.Errorf("%w: no token", ErrNoCloudAccess)
	h.cmd(model.CommandLock, "")
	h.step(25 * time.Second)
	if st := h.lastStatus(); st.Status != model.StatusFailed || *st.Error != model.FailUnreachable {
		t.Fatalf("status %+v", st)
	}
	if got := h.failedCommands(); !slices.Equal(got, []string{model.FailUnreachable}) {
		t.Fatalf("command_failed %v", got)
	}
}

func TestBridgeRecoveringBeforeCutoffNeedsNoCloud(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.bridge.commandErrs = unreachable(4)
	h.cmd(model.CommandUnlock, "")
	h.step(10 * time.Second)
	if len(h.bridge.commands) != 5 || len(h.cloud.commands) != 0 {
		t.Fatalf("bridge %v cloud %v", h.bridge.commands, h.cloud.commands)
	}
	if st := h.lastStatus(); st.Status != model.StatusSent || *st.Via != model.ViaLocal || st.Attempts != 5 {
		t.Fatalf("status %+v", st)
	}
	if h.s.httpFailures != 0 || h.s.mode != model.ModeLocal {
		t.Fatalf("failures %d mode %s", h.s.httpFailures, h.s.mode)
	}
}

func TestBridgeMayHaveActedIsNeverResent(t *testing.T) {
	cases := map[string]struct {
		err   error
		class string
	}{
		"no response": {fmt.Errorf("%w: read timeout", loqed.ErrNoResponse), model.FailNoResponse},
		"5xx":         {&loqed.APIError{StatusCode: 502}, model.FailNoResponse},
		"4xx":         {&loqed.APIError{StatusCode: 400}, model.FailRejected},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, testRecord(), config.LockSetting{})
			h.start()
			h.bridge.commandErrs = []error{c.err}
			h.cmd(model.CommandOpen, "x")
			h.step(15 * time.Second)
			if len(h.bridge.commands) != 1 || len(h.cloud.commands) != 0 {
				t.Fatalf("resent: bridge %v cloud %v", h.bridge.commands, h.cloud.commands)
			}
			h.wantTrail("pending@x", "sending@x", "failed/"+c.class+"@x")
			if got := h.failedCommands(); !slices.Equal(got, []string{c.class}) {
				t.Fatalf("command_failed %v", got)
			}
		})
	}
}

func TestCommandWithoutResponseIsConfirmedLater(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.bridge.commandErrs = []error{loqed.ErrNoResponse}
	h.cmd(model.CommandOpen, "x")
	h.advance(4 * time.Second)
	h.send(reached("STATE_CHANGED_OPEN", ourKey))
	if st := h.lastStatus(); st.Status != model.StatusConfirmed || st.Error != nil {
		t.Fatalf("a late confirmation must be reported: %+v", st)
	}
}

func TestCommandWithoutResponseReadsStatusAfterThirtySeconds(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.bridge.commandErrs = []error{loqed.ErrNoResponse}
	h.cmd(model.CommandOpen, "")
	h.bridge.status.BoltState = loqed.BoltOpen
	h.run(29 * time.Second)
	if h.bridge.statusCalls != 1 {
		t.Fatalf("too early: %d", h.bridge.statusCalls)
	}
	h.run(time.Second)
	if h.bridge.statusCalls != 2 || h.lock() != "OPEN" || h.lastStatus().Status != model.StatusConfirmed {
		t.Fatalf("status %d lock %s %+v", h.bridge.statusCalls, h.lock(), h.lastStatus())
	}
}

func TestCommandWithoutResponseFallsBackToCloudConfirm(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.bridge.commandErrs = []error{loqed.ErrNoResponse}
	h.cmd(model.CommandLock, "")
	h.bridge.statusErr = loqed.ErrNoResponse
	h.run(30 * time.Second) // the confirmation read fails
	calls := len(h.cloud.calls)
	h.run(6 * time.Second)
	if len(h.cloud.calls) != calls+1 || h.cloud.calls[calls] != PriorityConfirm || len(h.cloud.commands) != 0 {
		t.Fatalf("calls %v commands %v", h.cloud.calls, h.cloud.commands)
	}
}

func TestBridgeAuthErrorUsesCloudThenRefreshes(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.bridge.commandErrs = []error{loqed.ErrUnauthorized}
	h.cmd(model.CommandUnlock, "")
	if len(h.bridge.commands) != 1 || len(h.cloud.commands) != 1 || *h.lastStatus().Via != model.ViaCloud {
		t.Fatalf("bridge %v cloud %v", h.bridge.commands, h.cloud.commands)
	}
	if len(h.refreshes) != 0 {
		t.Fatalf("the refresh waits until the command resolves: %v", h.refreshes)
	}
	h.send(CloudEventMsg{Event: cloud.WebhookEvent{Kind: cloud.KindStateReached, EventType: "STATE_CHANGED_LATCH",
		BoltState: loqed.BoltDayLock, KeyLocalID: ourKey}})
	h.send(reached("STATE_CHANGED_LATCH", ourKey))
	if h.lastStatus().Status != model.StatusConfirmed || !slices.Equal(h.refreshes, []Reason{ReasonUnauthorized}) {
		t.Fatalf("status %+v refreshes %v", h.lastStatus(), h.refreshes)
	}
}

func TestUnusableRefreshAfterAuthErrorLeavesLocalWithoutPanic(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	broken := testRecord()
	broken.LocalID = nil // the cloud stopped reporting local credentials
	h.refreshRec = &broken
	h.bridge.commandErrs = []error{loqed.ErrUnauthorized}
	h.cmd(model.CommandLock, "")
	h.send(reached("STATE_CHANGED_NIGHT_LOCK", ourKey))
	if h.s.mode != model.ModeCloud || h.s.bridge != nil {
		t.Fatalf("mode %s bridge %v", h.s.mode, h.s.bridge)
	}
	h.run(30 * time.Second) // ticks must not touch the nil bridge
	if len(h.cloud.commands) != 1 {
		t.Fatalf("cloud %v", h.cloud.commands)
	}
}

func TestCommandArrivingAfterItsDeadlineExpires(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.send(CommandMsg{Command: model.CommandOpen, ID: "x", At: h.now.Add(-11 * time.Second)})
	if len(h.bridge.commands) != 0 || len(h.cloud.commands) != 0 {
		t.Fatal("a stale OPEN must never fire")
	}
	h.wantTrail("pending@x", "expired@x")
	if got := h.failedCommands(); !slices.Equal(got, []string{model.FailExpired}) {
		t.Fatalf("command_failed %v", got)
	}
}

func TestQueuedCommandExpiresBehindSlowConfirmation(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.bridge.statusErr = loqed.ErrNoResponse
	h.cmd(model.CommandLock, "a")
	h.advance(time.Second)
	h.cmd(model.CommandOpen, "b") // 10 s deadline, waits for "a"
	h.run(15 * time.Second)
	if len(h.bridge.commands) != 1 {
		t.Fatalf("an expired command must never be sent: %v", h.bridge.commands)
	}
	if !slices.Contains(h.trail(), "expired@b") {
		t.Fatalf("trail %v", h.trail())
	}
}

func TestCommandPastLocalCutoffGoesStraightToCloud(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.send(CommandMsg{Command: model.CommandOpen, At: h.now.Add(-8 * time.Second)})
	if len(h.bridge.commands) != 0 || len(h.cloud.commands) != 1 {
		t.Fatalf("bridge %v cloud %v", h.bridge.commands, h.cloud.commands)
	}
	if d := h.cloud.cmdBudgets[0]; d > 2*time.Second {
		t.Fatalf("the cloud call carries only the remaining time: %v", d)
	}
}

func TestUnusableBridgeClientGoesStraightToCloud(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.s.bridge = nil
	h.newBridgeErr = errors.New("bad key encoding")
	h.cmd(model.CommandLock, "")
	if len(h.bridge.commands) != 0 || len(h.cloud.commands) != 1 {
		t.Fatalf("bridge %v cloud %v", h.bridge.commands, h.cloud.commands)
	}
}

// --- deliverCloud ---

func TestCloudModeSendsViaCloudAndPollsOnlyFreshData(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.toCloud()
	calls := len(h.cloud.calls)
	at := h.now
	h.cmd(model.CommandUnlock, "")
	if len(h.cloud.commands) != 1 || h.cloud.commands[0] != loqed.BoltDayLock || h.lock() != "UNLOCKING" {
		t.Fatalf("cloud %v lock %s", h.cloud.commands, h.lock())
	}
	if st := h.lastStatus(); st.Status != model.StatusSent || *st.Via != model.ViaCloud {
		t.Fatalf("status %+v", st)
	}
	h.run(5 * time.Second)
	if len(h.cloud.calls) != calls+1 || h.cloud.calls[calls] != PriorityConfirm || !h.cloud.notBefore[calls].Equal(at) {
		t.Fatalf("calls %v notBefore %v", h.cloud.calls, h.cloud.notBefore)
	}
}

func TestCloudCommandErrors(t *testing.T) {
	cases := map[string]struct {
		err     error
		class   string
		confirm bool
		refresh bool
	}{
		"key deleted":  {errors.Join(cloud.ErrKeyDeleted, &loqed.APIError{StatusCode: 404}), model.FailKeyDeleted, false, true},
		"no response":  {loqed.ErrNoResponse, model.FailNoResponse, true, false},
		"5xx":          {&loqed.APIError{StatusCode: 503}, model.FailNoResponse, true, false},
		"rate limited": {loqed.ErrRateLimited, model.FailRateLimited, false, false},
		"unauthorized": {loqed.ErrUnauthorized, model.FailUnauthorized, false, false},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, testRecord(), config.LockSetting{})
			h.start()
			h.toCloud()
			h.cloud.commandErr = c.err
			h.cloud.locks[0].BoltState = loqed.BoltDayLock // a poll must not confirm it
			calls, refreshes := len(h.cloud.calls), len(h.refreshes)
			h.cmd(model.CommandLock, "")
			h.run(6 * time.Second)
			if len(h.cloud.commands) != 1 {
				t.Fatalf("resent: %v", h.cloud.commands)
			}
			if st := h.lastStatus(); st.Status != model.StatusFailed || *st.Error != c.class {
				t.Fatalf("status %+v", st)
			}
			if polled := slices.Contains(h.cloud.calls[calls:], PriorityConfirm); polled != c.confirm {
				t.Fatalf("confirmation poll %v, want %v", polled, c.confirm)
			}
			if refreshed := len(h.refreshes) > refreshes; refreshed != c.refresh {
				t.Fatalf("refresh %v, want %v", h.refreshes, c.refresh)
			}
		})
	}
}

// --- confirm ---

func TestGoToWithOurKeyIsAccepted(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.cmd(model.CommandLock, "")
	h.send(goTo("GO_TO_STATE_MANUAL_LOCK_REMOTE_NIGHT_LOCK", loqed.BoltNightLock)) // key 3: someone else
	if h.lastStatus().Status != model.StatusSent {
		t.Fatalf("another key does not accept our command: %+v", h.lastStatus())
	}
	h.send(ourGoTo(loqed.BoltNightLock))
	if h.lastStatus().Status != model.StatusAccepted {
		t.Fatalf("status %+v", h.lastStatus())
	}
	if ev := h.pub.events[len(h.pub.events)-1]; ev.Source != model.SourceGateway {
		t.Fatalf("our key during our command is the gateway: %+v", ev)
	}
}

func TestStateChangedReachingTargetConfirms(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.cmd(model.CommandLock, "")
	h.send(ourGoTo(loqed.BoltNightLock))
	h.send(reached("STATE_CHANGED_NIGHT_LOCK", nil))
	if h.lastStatus().Status != model.StatusConfirmed || h.lock() != "LOCKED" {
		t.Fatalf("status %+v lock %s", h.lastStatus(), h.lock())
	}
	h.run(2 * time.Minute)
	if h.bridge.statusCalls != 1 || h.lastStatus().Status != model.StatusConfirmed {
		t.Fatalf("no confirmation read after the webhook: %d %+v", h.bridge.statusCalls, h.lastStatus())
	}
}

func TestAcceptedInTargetStateConfirmsAfterGrace(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start() // day_lock
	h.cmd(model.CommandUnlock, "")
	h.send(BridgeEventMsg{Event: bridge.GoToStateEvent{EventType: "GO_TO_STATE_MANUAL_UNLOCK_REMOTE_LATCH",
		GoToState: loqed.BoltDayLock, KeyLocalID: ourKey}})
	h.advance(4 * time.Second)
	if h.lastStatus().Status != model.StatusAccepted {
		t.Fatalf("too early: %+v", h.lastStatus())
	}
	h.advance(time.Second)
	if h.lastStatus().Status != model.StatusConfirmed || h.lock() != "UNLOCKED" {
		t.Fatalf("status %+v lock %s", h.lastStatus(), h.lock())
	}
}

func TestMotorStallFailsTheCommand(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.cmd(model.CommandLock, "")
	h.send(ourGoTo(loqed.BoltNightLock))
	h.send(reached("MOTOR_STALL", ourKey))
	if st := h.lastStatus(); st.Status != model.StatusFailed || *st.Error != model.FailStalled || h.lock() != "JAMMED" {
		t.Fatalf("status %+v lock %s", st, h.lock())
	}
	if got := h.failedCommands(); !slices.Equal(got, []string{model.FailStalled}) {
		t.Fatalf("command_failed %v", got)
	}
}

func TestNoConfirmationWithinThirtySeconds(t *testing.T) {
	cases := map[string]struct {
		bolt   loqed.BoltState
		status model.CommandStatusValue
		stale  bool
		lock   string
	}{
		"status shows the target": {loqed.BoltNightLock, model.StatusConfirmed, false, "LOCKED"},
		"status shows no change":  {loqed.BoltDayLock, model.StatusFailed, true, "UNLOCKED"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, testRecord(), config.LockSetting{})
			h.start() // day_lock
			h.cmd(model.CommandLock, "")
			h.bridge.status.BoltState = c.bolt
			h.run(30 * time.Second)
			st := h.lastStatus()
			if st.Status != c.status || h.state().StateStale != c.stale || h.lock() != c.lock {
				t.Fatalf("status %+v stale %v lock %s", st, h.state().StateStale, h.lock())
			}
			if c.status == model.StatusFailed && *st.Error != model.FailNoConfirmation {
				t.Fatalf("error %v", *st.Error)
			}
		})
	}
}

func TestCloudPollShowingTargetConfirms(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.toCloud()
	h.cmd(model.CommandLock, "")
	h.cloud.locks[0].BoltState = loqed.BoltNightLock
	h.run(5 * time.Second)
	if h.lastStatus().Status != model.StatusConfirmed || h.lock() != "LOCKED" {
		t.Fatalf("status %+v lock %s", h.lastStatus(), h.lock())
	}
}

// The cloud may still report the pre-command state at the first
// confirmation poll; one more poll catches up, a second miss fails.
func TestLaggingCloudConfirmation(t *testing.T) {
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
	h.cmd(model.CommandLock, "")
	h.run(5 * time.Second)
	if confirms() != 1 || h.lock() != "UNLOCKED" || !h.state().StateStale || h.lastStatus().Status != model.StatusSent {
		t.Fatalf("lagging poll: confirms %d lock %s stale %v %+v", confirms(), h.lock(), h.state().StateStale, h.lastStatus())
	}
	h.run(5 * time.Second)
	if confirms() != 2 || h.lastStatus().Status != model.StatusFailed || *h.lastStatus().Error != model.FailNoConfirmation {
		t.Fatalf("second miss: confirms %d %+v", confirms(), h.lastStatus())
	}
	h.run(30 * time.Second)
	if confirms() != 2 {
		t.Fatalf("confirms %d", confirms())
	}
}

func TestLaggingCloudConfirmationCatchesUp(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.cloud.locks[0].BoltState = loqed.BoltDayLock
	h.toCloud()
	h.cmd(model.CommandLock, "")
	h.run(5 * time.Second)
	h.cloud.locks[0].BoltState = loqed.BoltNightLock
	h.run(5 * time.Second)
	if h.lock() != "LOCKED" || h.state().StateStale || h.lastStatus().Status != model.StatusConfirmed {
		t.Fatalf("lock %s stale %v %+v", h.lock(), h.state().StateStale, h.lastStatus())
	}
}

// The failure that trips failover must not lose the confirmation of a
// command that may have run.
func TestUncertainCommandOnFailoverIsStillConfirmed(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.s.httpFailures = 2
	h.bridge.commandErrs = []error{loqed.ErrNoResponse}
	h.cmd(model.CommandOpen, "")
	if h.s.mode != model.ModeCloud || len(h.cloud.commands) != 0 {
		t.Fatalf("mode %s cloud commands %v", h.s.mode, h.cloud.commands)
	}
	calls := len(h.cloud.calls)
	h.run(6 * time.Second)
	if !slices.Contains(h.cloud.calls[calls:], PriorityConfirm) {
		t.Fatalf("no confirm poll: %v", h.cloud.calls[calls:])
	}
}

// A background poll started by a failover that follows a cloud-sent command
// may carry data fetched before the command; it must not undo LOCKING.
func TestFallbackPollWithPreCommandDataKeepsMovingState(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.s.httpFailures = 2
	h.bridge.commandErrs = unreachable(100)
	h.cloud.locks[0].BoltState = loqed.BoltDayLock
	h.cmd(model.CommandLock, "")
	h.cloud.fetchedAt = h.now
	h.step(20 * time.Second)
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

// --- burst ---

func TestBurstWhileUnreachableDeliversOnlyTheLastCommand(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.bridge.commandErrs = unreachable(3)
	h.cmd(model.CommandUnlock, "a")
	h.advance(300 * time.Millisecond)
	h.cmd(model.CommandLock, "b")
	h.advance(300 * time.Millisecond)
	h.cmd(model.CommandUnlock, "c")
	h.step(3 * time.Second)
	delivered := h.bridge.commands[len(h.bridge.commands)-1]
	if len(h.bridge.commands) != 4 || delivered != bridge.ActionUnlock || len(h.cloud.commands) != 0 {
		t.Fatalf("bridge %v cloud %v", h.bridge.commands, h.cloud.commands)
	}
	trail := h.trail()
	for _, want := range []string{"superseded@a", "superseded@b", "sent@c"} {
		if !slices.Contains(trail, want) {
			t.Fatalf("missing %s in %v", want, trail)
		}
	}
	if h.s.httpFailures != 0 {
		t.Fatalf("superseded attempts must not count as bridge failures: %d", h.s.httpFailures)
	}
}

func TestNextWakeIsTheNextRetry(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.bridge.commandErrs = unreachable(1)
	h.cmd(model.CommandLock, "")
	if got := h.s.cmds.nextWake(); !got.Equal(h.now.Add(500 * time.Millisecond)) {
		t.Fatalf("next wake %v", got)
	}
	h.advance(500 * time.Millisecond)
	if !h.s.cmds.nextWake().IsZero() {
		t.Fatal("nothing to wake for once the command is written")
	}
}

// After a local→cloud fallback the confirmation must come from the cloud:
// the bridge is down, so a /status read would only fail.
func TestFallbackCommandIsConfirmedViaCloud(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.bridge.commandErrs = unreachable(100)
	h.cloud.locks[0].BoltState = loqed.BoltNightLock
	h.cmd(model.CommandUnlock, "")
	h.step(20 * time.Second)
	if len(h.cloud.commands) != 1 {
		t.Fatalf("cloud %v", h.cloud.commands)
	}
	h.cloud.locks[0].BoltState = loqed.BoltDayLock
	h.run(5 * time.Second)
	if h.lock() != "UNLOCKED" || h.cloud.calls[len(h.cloud.calls)-1] != PriorityConfirm || h.lastStatus().Status != model.StatusConfirmed {
		t.Fatalf("lock %s calls %v %+v", h.lock(), h.cloud.calls, h.lastStatus())
	}
}

func TestRejectedCommandIsNotConfirmedByLaterMovement(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.bridge.commandErrs = []error{&loqed.APIError{StatusCode: 400}}
	h.cmd(model.CommandLock, "")
	h.advance(10 * time.Second)
	h.send(reached("STATE_CHANGED_NIGHT_LOCK", model.Ptr(3))) // someone else locks
	if st := h.lastStatus(); st.Status != model.StatusFailed || *st.Error != model.FailRejected {
		t.Fatalf("status %+v", st)
	}
}

func TestUnconfirmedCommandIsConfirmedByLateWebhook(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.bridge.statusErr = loqed.ErrNoResponse
	h.cmd(model.CommandLock, "")
	h.run(31 * time.Second)
	if st := h.lastStatus(); st.Status != model.StatusFailed || *st.Error != model.FailNoConfirmation {
		t.Fatalf("status %+v", st)
	}
	h.send(reached("STATE_CHANGED_NIGHT_LOCK", ourKey))
	if st := h.lastStatus(); st.Status != model.StatusConfirmed || st.Error != nil {
		t.Fatalf("status %+v", st)
	}
}

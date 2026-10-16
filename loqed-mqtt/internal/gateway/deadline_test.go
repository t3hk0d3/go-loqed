package gateway

import (
	"slices"
	"testing"
	"time"

	"github.com/t3hk0d3/go-loqed/loqed-mqtt/internal/config"
	"github.com/t3hk0d3/go-loqed/loqed-mqtt/internal/model"
)

// slowSending makes the "sending" command_status publish take d.
func slowSending(h *harness, d time.Duration) {
	h.pub.onStatus = func(s model.CommandStatus) {
		if s.Status == model.StatusSending {
			h.now = h.now.Add(d)
		}
	}
}

func TestLocalAttemptPastCutoffAfterSendingPublishGoesToCloudInstead(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	slowSending(h, 8*time.Second) // OPEN: cutoff 7 s, deadline 10 s
	h.command(model.CommandOpen)
	if len(h.bridge.commands) != 0 {
		t.Fatalf("bridge got %v after the local cutoff", h.bridge.commands)
	}
	if len(h.cloud.commands) != 0 {
		t.Fatalf("cloud got %v although its sending publish also took 8 s (past the deadline)", h.cloud.commands)
	}
	if got := h.failedCommands(); !slices.Equal(got, []string{model.FailExpired}) {
		t.Fatalf("failed commands = %v, want [expired]", got)
	}
}

func TestLocalAttemptPastCutoffStillTriesCloudBeforeDeadline(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	slowed := false
	h.pub.onStatus = func(s model.CommandStatus) {
		if s.Status == model.StatusSending && !slowed {
			slowed = true
			h.now = h.now.Add(8 * time.Second) // only the local attempt's publish is slow
		}
	}
	h.command(model.CommandOpen)
	if len(h.bridge.commands) != 0 {
		t.Fatalf("bridge got %v after the local cutoff", h.bridge.commands)
	}
	if len(h.cloud.commands) != 1 {
		t.Fatalf("cloud commands = %v, want the OPEN sent before its deadline", h.cloud.commands)
	}
}

func TestCloudAttemptPastDeadlineAfterSendingPublishIsNotMade(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.toCloud()
	slowSending(h, 31*time.Second) // LOCK: deadline 30 s
	h.command(model.CommandLock)
	if len(h.cloud.commands) != 0 {
		t.Fatalf("cloud got %v after the deadline", h.cloud.commands)
	}
	if got := h.failedCommands(); !slices.Equal(got, []string{model.FailExpired}) {
		t.Fatalf("failed commands = %v, want [expired]", got)
	}
}

func TestRequestContextsEndAtCutoffAndDeadline(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	slowSending(h, 5*time.Second) // OPEN: 2 s of the 7 s local window left
	h.command(model.CommandOpen)
	if len(h.bridge.cmdBudgets) != 1 {
		t.Fatalf("bridge calls = %d, want 1", len(h.bridge.cmdBudgets))
	}
	// The context ends at the cutoff on the wall clock of the test; with the
	// fake clock the budget is measured against real time, so only its upper
	// bound is meaningful: at most the 2 s left, never the full 5 s timeout.
	if b := h.bridge.cmdBudgets[0]; b > 2*time.Second+time.Second {
		t.Fatalf("bridge request budget %v, want at most what was left of the local window", b)
	}
}

func TestCloudRequestContextEndsAtDeadline(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.toCloud()
	slowSending(h, 20*time.Second) // LOCK: 10 s of the 30 s left
	h.command(model.CommandLock)
	if len(h.cloud.cmdBudgets) != 1 {
		t.Fatalf("cloud calls = %d, want 1", len(h.cloud.cmdBudgets))
	}
	if b := h.cloud.cmdBudgets[0]; b > 10*time.Second || b < 9*time.Second {
		t.Fatalf("cloud request budget %v, want the 10 s left until the deadline", b)
	}
}

package gateway

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/cloud"
	"github.com/t3hk0d3/go-loqed/loqed-mqtt/internal/store"
)

type scriptedAPI struct {
	token    string
	errs     []error // consumed per ListLocks call
	calls    int
	commands int
}

func (a *scriptedAPI) ListLocks(context.Context) ([]cloud.Lock, error) {
	a.calls++
	if len(a.errs) > 0 {
		err := a.errs[0]
		a.errs = a.errs[1:]
		if err != nil {
			return nil, err
		}
	}
	return []cloud.Lock{{ID: "lock1"}}, nil
}

func (a *scriptedAPI) Command(context.Context, string, loqed.BoltState) error {
	a.commands++
	switch a.token {
	case "old":
		return loqed.ErrUnauthorized
	case "keyless":
		return errors.Join(cloud.ErrKeyDeleted, &loqed.APIError{StatusCode: 404})
	}
	return nil
}

type fakeTokens struct {
	token, next string
	err         error
	keyDeleted  []string // tokens reported via KeyDeleted
}

func (f *fakeTokens) Token(context.Context) (string, error) { return f.token, f.err }
func (f *fakeTokens) Invalidate(_ context.Context, rejected string) (string, error) {
	if f.next == "" {
		return "", loqed.ErrUnauthorized
	}
	f.token = f.next
	return f.next, nil
}

func (f *fakeTokens) KeyDeleted(_ context.Context, token string) (string, error) {
	f.keyDeleted = append(f.keyDeleted, token)
	if f.next == "" {
		return "", errors.New("no replacement")
	}
	f.token = f.next
	return f.next, nil
}

func newHub(now *time.Time, tokens TokenSource, apis map[string]*scriptedAPI) *CloudHub {
	clock := func() time.Time { return *now }
	return NewCloudHub(NewBudget(10, 12*time.Hour, clock, store.BudgetState{}, nil), tokens, func(tok string) CloudAPI {
		a := apis[tok]
		a.token = tok
		return a
	}, clock, slog.New(slog.DiscardHandler))
}

func TestHubCoalescesRequests(t *testing.T) {
	now := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	api := &scriptedAPI{}
	h := newHub(&now, &fakeTokens{token: "tok"}, map[string]*scriptedAPI{"tok": api})
	ctx := context.Background()
	for range 3 {
		if _, err := h.Locks(ctx, PriorityRefresh, time.Time{}); err != nil {
			t.Fatal(err)
		}
	}
	if api.calls != 1 {
		t.Fatalf("calls %d", api.calls)
	}
	now = now.Add(31 * time.Second)
	list, _ := h.Locks(ctx, PriorityRefresh, time.Time{})
	if api.calls != 2 || h.Token() != "tok" || !list.FetchedAt.Equal(now) {
		t.Fatalf("calls %d token %q fetched %v", api.calls, h.Token(), list.FetchedAt)
	}
}

// A confirmation poll must never be answered from data fetched before the
// command (that showed LOCKED for an hour after an UNLOCK).
func TestHubConfirmIgnoresOlderCachedResult(t *testing.T) {
	now := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	api := &scriptedAPI{}
	h := newHub(&now, &fakeTokens{token: "tok"}, map[string]*scriptedAPI{"tok": api})
	ctx := context.Background()
	_, _ = h.Locks(ctx, PriorityBackground, time.Time{})
	now = now.Add(10 * time.Second)
	commandAt := now
	now = now.Add(5 * time.Second)
	list, err := h.Locks(ctx, PriorityConfirm, commandAt)
	if err != nil || api.calls != 2 || list.FetchedAt.Before(commandAt) {
		t.Fatalf("calls %d fetched %v err %v", api.calls, list.FetchedAt, err)
	}
}

func TestHubTakesBudgetOnlyWhenSending(t *testing.T) {
	now := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	tokens := &fakeTokens{err: errors.New("mint refused")}
	h := newHub(&now, tokens, map[string]*scriptedAPI{})
	for range 5 {
		_, _ = h.Locks(context.Background(), PriorityConfirm, time.Time{})
	}
	if h.Budget().Remaining() != 10 {
		t.Fatalf("budget spent without requests: %d left", h.Budget().Remaining())
	}
}

func TestHubReauthenticatesOnce(t *testing.T) {
	now := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	old := &scriptedAPI{errs: []error{loqed.ErrUnauthorized}}
	fresh := &scriptedAPI{}
	h := newHub(&now, &fakeTokens{token: "old", next: "new"}, map[string]*scriptedAPI{"old": old, "new": fresh})
	list, err := h.Locks(context.Background(), PriorityRefresh, time.Time{})
	if err != nil || len(list.Locks) != 1 || fresh.calls != 1 || h.Token() != "new" {
		t.Fatalf("locks %v err %v fresh %d token %q", list, err, fresh.calls, h.Token())
	}
}

func TestHubRateLimitBlocksBudget(t *testing.T) {
	now := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	api := &scriptedAPI{errs: []error{loqed.ErrRateLimited}}
	h := newHub(&now, &fakeTokens{token: "tok"}, map[string]*scriptedAPI{"tok": api})
	if _, err := h.Locks(context.Background(), PriorityRefresh, time.Time{}); !errors.Is(err, loqed.ErrRateLimited) {
		t.Fatalf("got %v", err)
	}
	now = now.Add(time.Hour)
	if _, err := h.Locks(context.Background(), PriorityRefresh, time.Time{}); !errors.Is(err, ErrCloudBlocked) {
		t.Fatalf("got %v", err)
	}
	if api.calls != 1 {
		t.Fatalf("blocked hub must not call the API: %d", api.calls)
	}
}

func TestHubCommandReauthenticatesOutsideTheBudget(t *testing.T) {
	now := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	old, fresh := &scriptedAPI{}, &scriptedAPI{}
	h := newHub(&now, &fakeTokens{token: "old", next: "new"}, map[string]*scriptedAPI{"old": old, "new": fresh})
	if err := h.Command(context.Background(), "lock1", loqed.BoltNightLock); err != nil {
		t.Fatal(err)
	}
	if old.commands != 1 || fresh.commands != 1 || h.Budget().Remaining() != 10 {
		t.Fatalf("old %d fresh %d remaining %d", old.commands, fresh.commands, h.Budget().Remaining())
	}
}

// A slow read must not hold up a door command.
func TestHubCommandDoesNotWaitForReads(t *testing.T) {
	now := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	api := &scriptedAPI{}
	h := newHub(&now, &fakeTokens{token: "tok"}, map[string]*scriptedAPI{"tok": api})
	h.reads <- struct{}{} // a read is in progress
	defer func() { <-h.reads }()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := h.Command(ctx, "lock1", loqed.BoltOpen); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Locks(ctx, PriorityConfirm, time.Time{}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("reads wait for the in-flight read, bounded by ctx: %v", err)
	}
}

func TestHubCommandDeletedKeyIsNotResent(t *testing.T) {
	now := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	keyless, fresh := &scriptedAPI{}, &scriptedAPI{}
	tokens := &fakeTokens{token: "keyless", next: "fresh"}
	h := newHub(&now, tokens, map[string]*scriptedAPI{"keyless": keyless, "fresh": fresh})
	err := h.Command(context.Background(), "lock1", loqed.BoltOpen)
	if !errors.Is(err, cloud.ErrKeyDeleted) {
		t.Fatalf("got %v", err)
	}
	if keyless.commands != 1 || fresh.commands != 0 {
		t.Fatalf("resent: keyless=%d fresh=%d", keyless.commands, fresh.commands)
	}
	if len(tokens.keyDeleted) != 1 || tokens.keyDeleted[0] != "keyless" {
		t.Fatalf("token source not told: %v", tokens.keyDeleted)
	}
	if err := h.Command(context.Background(), "lock1", loqed.BoltOpen); err != nil || fresh.commands != 1 {
		t.Fatalf("next command must use the replacement: %v fresh=%d", err, fresh.commands)
	}
}

func TestHubResetTokenResolvesAgain(t *testing.T) {
	now := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	old, fresh := &scriptedAPI{}, &scriptedAPI{}
	tokens := &fakeTokens{token: "a"}
	h := newHub(&now, tokens, map[string]*scriptedAPI{"a": old, "b": fresh})
	if err := h.Command(context.Background(), "lock1", loqed.BoltOpen); err != nil || old.commands != 1 {
		t.Fatal(err)
	}
	tokens.token = "b" // re-minted elsewhere
	h.ResetToken()
	if err := h.Command(context.Background(), "lock1", loqed.BoltOpen); err != nil || fresh.commands != 1 || h.Token() != "b" {
		t.Fatalf("err %v fresh %d token %q", err, fresh.commands, h.Token())
	}
}

// LOQED does not count reads it rejects as unauthenticated (spec 2.5, V12), so
// a wrong token must not use up the budget, e.g. in a restart loop.
func TestHubRejectedReadsCostNoBudget(t *testing.T) {
	now := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	errs := make([]error, 30)
	for i := range errs {
		errs[i] = loqed.ErrUnauthorized
	}
	api := &scriptedAPI{errs: errs}
	h := newHub(&now, &fakeTokens{token: "bad"}, map[string]*scriptedAPI{"bad": api})
	for range 15 {
		if _, err := h.Locks(context.Background(), PriorityConfirm, time.Time{}); !errors.Is(err, loqed.ErrUnauthorized) {
			t.Fatalf("got %v", err)
		}
		now = now.Add(time.Minute)
	}
	if api.calls != 15 || h.Budget().Remaining() != 10 {
		t.Fatalf("calls %d, budget left %d; want 15 calls and the full budget", api.calls, h.Budget().Remaining())
	}
}

func TestHubChargesOnlyTheReadThatSucceedsAfterReauth(t *testing.T) {
	now := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	old := &scriptedAPI{errs: []error{loqed.ErrUnauthorized}}
	fresh := &scriptedAPI{}
	h := newHub(&now, &fakeTokens{token: "old", next: "new"}, map[string]*scriptedAPI{"old": old, "new": fresh})
	if _, err := h.Locks(context.Background(), PriorityRefresh, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if h.Budget().Remaining() != 9 {
		t.Fatalf("budget left %d, want 9", h.Budget().Remaining())
	}
}

package gateway

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/cloud"
	"github.com/t3hk0d3/go-loqed/internal/store"
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
	if a.token == "old" {
		return loqed.ErrUnauthorized
	}
	return nil
}

type fakeTokens struct {
	token, next string
	err         error
}

func (f *fakeTokens) Token(context.Context) (string, error) { return f.token, f.err }
func (f *fakeTokens) Invalidate(_ context.Context, rejected string) (string, error) {
	if f.next == "" {
		return "", loqed.ErrUnauthorized
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

func TestHubCommandReauthenticatesAndIsRecorded(t *testing.T) {
	now := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	old, fresh := &scriptedAPI{}, &scriptedAPI{}
	h := newHub(&now, &fakeTokens{token: "old", next: "new"}, map[string]*scriptedAPI{"old": old, "new": fresh})
	if err := h.Command(context.Background(), "lock1", loqed.BoltNightLock); err != nil {
		t.Fatal(err)
	}
	if old.commands != 1 || fresh.commands != 1 || h.Budget().Remaining() != 8 {
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

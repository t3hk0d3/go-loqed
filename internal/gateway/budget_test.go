package gateway

import (
	"errors"
	"testing"
	"time"

	"github.com/t3hk0d3/go-loqed/internal/store"
)

func newBudget(limit int, now *time.Time) *Budget {
	return NewBudget(limit, 12*time.Hour, func() time.Time { return *now }, store.BudgetState{}, nil)
}

func TestBudgetLimitsCallsPerWindow(t *testing.T) {
	now := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	b := newBudget(3, &now)
	for i := range 3 {
		if err := b.Take(PriorityConfirm); err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
	}
	if err := b.Take(PriorityConfirm); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("got %v", err)
	}
	now = now.Add(12 * time.Hour)
	if err := b.Take(PriorityConfirm); err != nil {
		t.Fatalf("window should have rolled: %v", err)
	}
}

func TestBudgetReservesPerPriority(t *testing.T) {
	now := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	b := newBudget(10, &now)
	for range 8 {
		if err := b.Take(PriorityConfirm); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Take(PriorityBackground); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("background must leave 2: %v", err)
	}
	if err := b.Take(PriorityRefresh); err != nil {
		t.Fatalf("refresh may use the 9th: %v", err)
	}
	if err := b.Take(PriorityRefresh); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("refresh must leave 1 for confirmations: %v", err)
	}
	if err := b.Take(PriorityConfirm); err != nil {
		t.Fatalf("confirm may use the last call: %v", err)
	}
}

func TestBudgetBackgroundSpacing(t *testing.T) {
	now := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	b := newBudget(10, &now)
	if err := b.Take(PriorityBackground); err != nil {
		t.Fatal(err)
	}
	if err := b.Take(PriorityBackground); !errors.Is(err, ErrDeferred) {
		t.Fatalf("second background poll within 72m must be deferred: %v", err)
	}
	now = now.Add(72 * time.Minute)
	if err := b.Take(PriorityBackground); err != nil {
		t.Fatal(err)
	}
	if b.Spacing() != 72*time.Minute {
		t.Fatalf("spacing %v", b.Spacing())
	}
}

func TestBudgetBlock(t *testing.T) {
	now := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	b := newBudget(10, &now)
	b.Block(12 * time.Hour)
	if err := b.Take(PriorityConfirm); !errors.Is(err, ErrCloudBlocked) {
		t.Fatalf("got %v", err)
	}
	now = now.Add(12 * time.Hour)
	if err := b.Take(PriorityConfirm); err != nil {
		t.Fatal(err)
	}
}

// Restarts must not reset the window: a crash loop would otherwise get the
// account blocked by LOQED.
func TestBudgetPersistsAcrossRestarts(t *testing.T) {
	now := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	var saved store.BudgetState
	save := func(s store.BudgetState) { saved = s }
	b := NewBudget(3, 12*time.Hour, func() time.Time { return now }, store.BudgetState{}, save)
	_ = b.Take(PriorityConfirm)
	_ = b.Take(PriorityConfirm)
	b.Block(time.Hour)
	if len(saved.Calls) != 2 || !saved.BlockedUntil.Equal(now.Add(time.Hour)) {
		t.Fatalf("saved %+v", saved)
	}
	restarted := NewBudget(3, 12*time.Hour, func() time.Time { return now }, saved, save)
	if err := restarted.Take(PriorityConfirm); !errors.Is(err, ErrCloudBlocked) {
		t.Fatalf("block lost on restart: %v", err)
	}
	now = now.Add(time.Hour)
	_ = restarted.Take(PriorityConfirm)
	if err := restarted.Take(PriorityConfirm); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("calls lost on restart: %v", err)
	}
}

func TestBudgetClampsFutureTimestamps(t *testing.T) {
	now := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	state := store.BudgetState{Calls: []time.Time{now.Add(48 * time.Hour)}, BlockedUntil: now.Add(100 * time.Hour)}
	b := NewBudget(3, 12*time.Hour, func() time.Time { return now }, state, nil)
	now = now.Add(12 * time.Hour)
	if b.Remaining() != 3 {
		t.Fatalf("a call stamped in the future must expire after one window: %d", b.Remaining())
	}
	if err := b.Take(PriorityConfirm); err != nil {
		t.Fatalf("block must be clamped to 12h: %v", err)
	}
}

func TestBudgetRecordCountsCommands(t *testing.T) {
	now := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	b := newBudget(10, &now)
	for range 9 {
		b.Record()
	}
	if n := b.Record(); n != 10 {
		t.Fatalf("count %d", n)
	}
	if err := b.Take(PriorityConfirm); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("commands must count toward reads: %v", err)
	}
}

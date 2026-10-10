// Package gateway runs one supervisor per lock: local-first operation,
// cloud fallback, offline handling, and the shared cloud request budget.
package gateway

import (
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/t3hk0d3/go-loqed/loqed-mqtt/internal/store"
)

type Priority int

const (
	PriorityConfirm    Priority = iota // confirm a command sent via cloud; may use the whole budget
	PriorityRefresh                    // credential refresh; leaves 1 call for confirmations
	PriorityBackground                 // periodic cloud-mode poll; leaves 2 and is spaced out
)

var (
	ErrBudgetExhausted = errors.New("gateway: cloud request budget exhausted")
	ErrDeferred        = errors.New("gateway: background cloud poll deferred to spread the budget")
	ErrCloudBlocked    = errors.New("gateway: cloud reads suspended after LOQED rate limiting")
)

// reserve is how many calls each priority must leave unused.
func reserve(p Priority, limit int) int {
	switch p {
	case PriorityRefresh:
		return min(1, limit-1)
	case PriorityBackground:
		return min(2, limit-1)
	default:
		return 0
	}
}

// Budget limits cloud calls per rolling window, account-wide. Its state is
// persisted through save after every change, so restarts (crash loops,
// watchdogs) cannot exceed LOQED's account limit.
type Budget struct {
	limit  int
	window time.Duration
	now    func() time.Time
	save   func(store.BudgetState) // may be nil

	mu             sync.Mutex
	calls          []time.Time
	lastBackground time.Time
	blockedUntil   time.Time
}

// NewBudget restores state (timestamps in the future are clamped to now,
// a block to at most RateLimitBackoff from now).
func NewBudget(limit int, window time.Duration, now func() time.Time, state store.BudgetState, save func(store.BudgetState)) *Budget {
	b := &Budget{limit: limit, window: window, now: now, save: save}
	t := now()
	for _, c := range state.Calls {
		if c.After(t) {
			c = t
		}
		b.calls = append(b.calls, c)
	}
	slices.SortFunc(b.calls, func(a, c time.Time) int { return a.Compare(c) })
	if n := len(b.calls); n > 0 {
		b.lastBackground = b.calls[n-1] // keep spacing across restarts
	}
	b.blockedUntil = state.BlockedUntil
	if limitUntil := t.Add(RateLimitBackoff); b.blockedUntil.After(limitUntil) {
		b.blockedUntil = limitUntil
	}
	b.prune(t)
	return b
}

// Take reserves one read call for priority p.
func (b *Budget) Take(p Priority) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	if now.Before(b.blockedUntil) {
		return ErrCloudBlocked
	}
	b.prune(now)
	remaining := b.limit - len(b.calls)
	if remaining <= reserve(p, b.limit) {
		return ErrBudgetExhausted
	}
	if p == PriorityBackground {
		spacing := b.window / time.Duration(b.limit)
		if !b.lastBackground.IsZero() && now.Sub(b.lastBackground) < spacing {
			return ErrDeferred
		}
		b.lastBackground = now
	}
	b.calls = append(b.calls, now)
	b.persistLocked()
	return nil
}

// Refund gives back the latest call, for a read LOQED rejected as
// unauthenticated: those do not count toward its limit (spec 2.5, V12).
// Callers must not take concurrently (CloudHub serializes reads).
func (b *Budget) Refund() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if n := len(b.calls); n > 0 {
		b.calls = b.calls[:n-1]
		b.persistLocked()
	}
}

func (b *Budget) Block(d time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.blockedUntil = b.now().Add(d)
	b.persistLocked()
}

func (b *Budget) Remaining() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.prune(b.now())
	return b.limit - len(b.calls)
}

// Spacing is the interval between background polls.
func (b *Budget) Spacing() time.Duration { return b.window / time.Duration(b.limit) }

func (b *Budget) prune(now time.Time) {
	cut := 0
	for cut < len(b.calls) && now.Sub(b.calls[cut]) >= b.window {
		cut++
	}
	b.calls = b.calls[cut:]
}

func (b *Budget) persistLocked() {
	if b.save != nil {
		b.save(store.BudgetState{Calls: slices.Clone(b.calls), BlockedUntil: b.blockedUntil})
	}
}

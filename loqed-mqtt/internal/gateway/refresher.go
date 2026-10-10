package gateway

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/t3hk0d3/go-loqed/loqed-mqtt/internal/store"
)

type Reason string

const (
	ReasonUnauthorized Reason = "unauthorized"
	ReasonUnreachable  Reason = "unreachable"
)

var (
	ErrRefreshThrottled = errors.New("gateway: credential refresh throttled")
	ErrEmptyLockList    = errors.New("gateway: the cloud returned no locks; keeping the cached lock data")
)

const (
	refreshBackoffMin = 5 * time.Minute
	refreshBackoffMax = 6 * time.Hour
)

type LockLister interface {
	Locks(ctx context.Context, p Priority, notBefore time.Time) (LockList, error)
	Token() string
}

type backoff struct {
	next     time.Time
	interval time.Duration
}

// Refresher reloads lock credentials from the cloud into the store.
type Refresher struct {
	cloud LockLister
	store *store.Store
	now   func() time.Time

	// OnRemoved, if set, is called with the ids of locks that a refresh
	// removed from the cache (no longer on the account).
	OnRemoved func(ids []string)

	mu    sync.Mutex
	state map[string]*backoff // per lock+reason
}

func NewRefresher(c LockLister, st *store.Store, now func() time.Time) *Refresher {
	return &Refresher{cloud: c, store: st, now: now, state: map[string]*backoff{}}
}

// RefreshAll fetches the lock list and merges it into the cache: records
// keep cached local credentials the cloud no longer reports, locks absent
// from a non-empty list are removed, and an empty list is not applied
// while the cache has locks.
func (r *Refresher) RefreshAll(ctx context.Context) ([]store.LockRecord, error) {
	list, err := r.cloud.Locks(ctx, PriorityRefresh, time.Time{})
	if err != nil {
		return nil, err
	}
	before := r.store.Snapshot()
	if len(list.Locks) == 0 && len(before.Locks) > 0 {
		return nil, ErrEmptyLockList
	}
	recs := make([]store.LockRecord, 0, len(list.Locks))
	kept := map[string]bool{}
	for _, l := range list.Locks {
		rec := store.FromCloud(l)
		if old, ok := before.Find(l.ID); ok && old.ID == l.ID {
			rec = store.Merge(old, rec)
		}
		recs = append(recs, rec)
		kept[rec.ID] = true
	}
	var removed []string
	for _, old := range before.Locks {
		if !kept[old.ID] {
			removed = append(removed, old.ID)
		}
	}
	err = r.store.Update(func(c *store.Cache) {
		c.Locks = recs
		c.FetchedAt = r.now().UTC()
		c.TokenSHA256 = store.TokenHash(r.cloud.Token())
	})
	if len(removed) > 0 && r.OnRemoved != nil {
		r.OnRemoved(removed)
	}
	return recs, err // a store.ErrWrite still returns the fresh records
}

// Refresh refreshes for one lock and reason. Attempts back off
// exponentially (5 min doubling to 6 h) while refreshes return the same
// local credentials for the lock; a change resets the backoff.
func (r *Refresher) Refresh(ctx context.Context, lockID string, reason Reason) (store.LockRecord, error) {
	key := lockID + "/" + string(reason)
	now := r.now()
	r.mu.Lock()
	b := r.state[key]
	if b == nil {
		b = &backoff{interval: refreshBackoffMin}
		r.state[key] = b
	}
	if now.Before(b.next) {
		r.mu.Unlock()
		return store.LockRecord{}, ErrRefreshThrottled
	}
	b.next = now.Add(b.interval)
	r.mu.Unlock()

	old, _ := r.store.Snapshot().Find(lockID)
	recs, err := r.RefreshAll(ctx)
	if recs == nil {
		r.mu.Lock()
		b.interval = min(2*b.interval, refreshBackoffMax)
		b.next = now.Add(b.interval)
		r.mu.Unlock()
		return store.LockRecord{}, err
	}
	for _, rec := range recs {
		if rec.ID != lockID {
			continue
		}
		r.mu.Lock()
		if store.SameLocal(old, rec) {
			b.interval = min(2*b.interval, refreshBackoffMax)
		} else {
			b.interval = refreshBackoffMin
		}
		b.next = now.Add(b.interval)
		r.mu.Unlock()
		return rec, nil
	}
	return store.LockRecord{}, fmt.Errorf("gateway: lock %s is no longer on the account", lockID)
}

// RefreshIfOlder refreshes when cache_max_age (> 0) has passed since the
// last fetch. It reports whether a refresh ran.
func (r *Refresher) RefreshIfOlder(ctx context.Context, maxAge time.Duration) (bool, error) {
	if maxAge <= 0 || r.now().Sub(r.store.Snapshot().FetchedAt) < maxAge {
		return false, nil
	}
	_, err := r.RefreshAll(ctx)
	return true, err
}

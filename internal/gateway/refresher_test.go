package gateway

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/t3hk0d3/go-loqed/cloud"
	"github.com/t3hk0d3/go-loqed/internal/config"
	"github.com/t3hk0d3/go-loqed/internal/store"
)

type listerStub struct {
	locks []cloud.Lock
	calls int
	err   error
}

func (l *listerStub) Locks(context.Context, Priority, time.Time) (LockList, error) {
	l.calls++
	return LockList{Locks: l.locks}, l.err
}
func (l *listerStub) Token() string { return "tok" }

func cloudLock(id, ip string) cloud.Lock {
	lid := 1
	return cloud.Lock{ID: id, Name: "Lock " + id, BridgeIP: ip, LocalID: &lid, KeySecret: "k", BridgeKey: "b"}
}

func newStore(t *testing.T) *store.Store {
	t.Helper()
	st, _, err := store.Open(filepath.Join(t.TempDir(), "locks.json"))
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func TestRefreshAllWritesCache(t *testing.T) {
	st := newStore(t)
	l := &listerStub{locks: []cloud.Lock{cloudLock("lock1", "192.0.2.4")}}
	now := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	recs, err := NewRefresher(l, st, func() time.Time { return now }).RefreshAll(context.Background())
	if err != nil || len(recs) != 1 {
		t.Fatalf("%v %v", recs, err)
	}
	c := st.Snapshot()
	if len(c.Locks) != 1 || c.TokenSHA256 != store.TokenHash("tok") || !c.FetchedAt.Equal(now) {
		t.Fatalf("%+v", c)
	}
}

func TestRefreshAllMergesAndRemoves(t *testing.T) {
	st := newStore(t)
	now := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	l := &listerStub{locks: []cloud.Lock{cloudLock("a", "192.0.2.4"), cloudLock("b", "192.0.2.5")}}
	r := NewRefresher(l, st, func() time.Time { return now })
	var removed []string
	r.OnRemoved = func(ids []string) { removed = ids }
	_, _ = r.RefreshAll(context.Background())

	// The cloud stops reporting local fields for "a" and drops "b".
	l.locks = []cloud.Lock{{ID: "a", Name: "Renamed"}}
	recs, err := r.RefreshAll(context.Background())
	if err != nil || len(recs) != 1 || recs[0].Name != "Renamed" || !recs[0].HasLocalCredentials() || recs[0].BridgeIP != "192.0.2.4" {
		t.Fatalf("%+v %v", recs, err)
	}
	if !slices.Equal(removed, []string{"b"}) {
		t.Fatalf("removed %v", removed)
	}
}

func TestRefreshAllIgnoresEmptyList(t *testing.T) {
	st := newStore(t)
	l := &listerStub{locks: []cloud.Lock{cloudLock("a", "192.0.2.4")}}
	r := NewRefresher(l, st, time.Now)
	_, _ = r.RefreshAll(context.Background())
	l.locks = nil
	if _, err := r.RefreshAll(context.Background()); !errors.Is(err, ErrEmptyLockList) {
		t.Fatalf("got %v", err)
	}
	if len(st.Snapshot().Locks) != 1 {
		t.Fatal("an empty list must not wipe the cache")
	}
}

func TestRefreshBacksOffWhileNothingChanges(t *testing.T) {
	st := newStore(t)
	l := &listerStub{locks: []cloud.Lock{cloudLock("lock1", "192.0.2.4")}}
	now := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	r := NewRefresher(l, st, func() time.Time { return now })
	ctx := context.Background()
	_, _ = r.RefreshAll(ctx) // cache primed
	// Same data every time: 5m, 10m, 20m, 40m ... capped at 6h.
	var allowed []time.Duration
	start := now
	for now.Sub(start) < 12*time.Hour {
		if _, err := r.Refresh(ctx, "lock1", ReasonUnreachable); err == nil {
			allowed = append(allowed, now.Sub(start))
		} else if !errors.Is(err, ErrRefreshThrottled) {
			t.Fatal(err)
		}
		now = now.Add(time.Minute)
	}
	if len(allowed) > 8 {
		t.Fatalf("a flapping bridge spent %d refreshes in 12h: %v", len(allowed), allowed)
	}
	if allowed[1]-allowed[0] != 10*time.Minute {
		t.Fatalf("second gap %v", allowed[1]-allowed[0])
	}
	// A different reason has its own schedule.
	if _, err := r.Refresh(ctx, "lock1", ReasonUnauthorized); err != nil {
		t.Fatalf("different reason must not be throttled: %v", err)
	}
	if _, err := r.Refresh(ctx, "missing", ReasonUnreachable); err == nil {
		t.Fatal("expected error for lock no longer on the account")
	}
}

func TestRefreshBackoffResetsWhenDataChanges(t *testing.T) {
	st := newStore(t)
	l := &listerStub{locks: []cloud.Lock{cloudLock("lock1", "192.0.2.4")}}
	now := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	r := NewRefresher(l, st, func() time.Time { return now })
	ctx := context.Background()
	_, _ = r.RefreshAll(ctx)
	_, _ = r.Refresh(ctx, "lock1", ReasonUnreachable) // unchanged → next in 10m
	now = now.Add(10 * time.Minute)
	l.locks[0].BridgeIP = "192.0.2.9"
	if rec, err := r.Refresh(ctx, "lock1", ReasonUnreachable); err != nil || rec.BridgeIP != "192.0.2.9" {
		t.Fatalf("%+v %v", rec, err)
	}
	now = now.Add(5 * time.Minute)
	if _, err := r.Refresh(ctx, "lock1", ReasonUnreachable); err != nil {
		t.Fatalf("a change must reset the backoff to 5m: %v", err)
	}
}

func TestRefreshIfOlder(t *testing.T) {
	st := newStore(t)
	l := &listerStub{locks: []cloud.Lock{cloudLock("lock1", "192.0.2.4")}}
	now := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	r := NewRefresher(l, st, func() time.Time { return now })
	_, _ = r.RefreshAll(context.Background())
	if ran, _ := r.RefreshIfOlder(context.Background(), 0); ran {
		t.Fatal("0 disables age refreshes")
	}
	now = now.Add(7 * 24 * time.Hour)
	if ran, err := r.RefreshIfOlder(context.Background(), 168*time.Hour); !ran || err != nil || l.calls != 2 {
		t.Fatalf("ran %v err %v calls %d", ran, err, l.calls)
	}
}

func TestSelectAndSettings(t *testing.T) {
	recs := []store.LockRecord{{ID: "a", Name: "Front door"}, {ID: "b", Name: "Back door"}}
	all, missing := Select(recs, nil)
	if len(all) != 2 || missing != nil {
		t.Fatalf("%v %v", all, missing)
	}
	sel, missing := Select(recs, []string{"Back door", "a", "Garage"})
	if len(sel) != 2 || sel[0].ID != "b" || sel[1].ID != "a" || len(missing) != 1 || missing[0] != "Garage" {
		t.Fatalf("%v %v", sel, missing)
	}

	id := 7
	settings := config.LockSettingsMap{"Front door": {BridgeIP: "10.0.0.9", LocalID: &id, KeyNames: config.KeyNames{1: "Alice"}}, "Shed": {}}
	s := SettingFor(settings, recs[0])
	got := ApplySetting(recs[0], s)
	if got.BridgeIP != "10.0.0.9" || *got.LocalID != 7 || got.Name != "Front door" {
		t.Fatalf("%+v", got)
	}
	if SettingFor(settings, recs[1]).BridgeIP != "" {
		t.Fatal("no setting expected for b")
	}
	if !KeysPinned(s) || !IPPinned(s) || KeysPinned(config.LockSetting{BridgeIP: "x"}) {
		t.Fatal("pinning helpers")
	}
	if u := UnmatchedSettings(settings, recs); !slices.Equal(u, []string{"Shed"}) {
		t.Fatalf("unmatched %v", u)
	}
}

func TestRefreshBacksOffWhenCloudFails(t *testing.T) {
	for name, setup := range map[string]func(l *listerStub){
		"error":      func(l *listerStub) { l.err = errors.New("boom") },
		"empty list": func(l *listerStub) { l.locks = nil },
	} {
		t.Run(name, func(t *testing.T) {
			st := newStore(t)
			l := &listerStub{locks: []cloud.Lock{cloudLock("lock1", "192.0.2.4")}}
			now := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
			r := NewRefresher(l, st, func() time.Time { return now })
			ctx := context.Background()
			_, _ = r.RefreshAll(ctx) // cache primed
			setup(l)
			l.calls = 0
			var attempts []time.Duration
			start := now
			for now.Sub(start) < 12*time.Hour {
				_, err := r.Refresh(ctx, "lock1", ReasonUnreachable)
				if err == nil {
					t.Fatal("expected an error")
				}
				if !errors.Is(err, ErrRefreshThrottled) {
					attempts = append(attempts, now.Sub(start))
				}
				now = now.Add(time.Minute)
			}
			if l.calls > 8 || len(attempts) != l.calls {
				t.Fatalf("%d cloud calls, attempts %v", l.calls, attempts)
			}
			if attempts[1]-attempts[0] != 10*time.Minute {
				t.Fatalf("second gap %v", attempts[1]-attempts[0])
			}
		})
	}
}

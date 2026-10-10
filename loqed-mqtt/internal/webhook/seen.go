package webhook

import (
	"sync"
	"time"
)

// MaxSeenPerLock caps how many applied bridge webhook deliveries are
// remembered per lock; past it the oldest is forgotten first. Only deliveries
// with a valid HASH are remembered, so the cap bounds memory to known locks
// times this, whatever is POSTed.
const MaxSeenPerLock = 256

// seenWarnWindow: repeats within this window are only counted and reported
// with the next log line (like the gateway's rate-limited warnings).
const seenWarnWindow = 10 * time.Minute

// seenDeliveries remembers the HASH of every applied bridge webhook, per
// lock, so a delivery that arrives again (the bridge sends one signed request
// to every registered URL, and anyone who saw it can resend it) is applied
// only once. HASH covers body and TIMESTAMP, so it identifies a delivery.
type seenDeliveries struct {
	retention time.Duration // 0: forgotten only at MaxSeenPerLock

	mu     sync.Mutex
	locks  map[string]*seenLock
	warned map[string]*seenWarn
}

type seenLock struct {
	expires map[string]time.Time
	order   []string // oldest first
}

type seenWarn struct {
	last       time.Time
	suppressed int
}

// newSeenDeliveries keeps a delivery for twice the timestamp tolerance (plus
// the whole-second rounding of the skew check), longer than its TIMESTAMP can
// pass the check. With the check off (0) nothing ever goes stale, so
// deliveries are kept until the cap pushes them out.
func newSeenDeliveries(tolerance time.Duration) *seenDeliveries {
	s := &seenDeliveries{locks: map[string]*seenLock{}, warned: map[string]*seenWarn{}}
	if tolerance > 0 {
		s.retention = 2*tolerance + 2*time.Second
	}
	return s
}

// claim records hash for lockID and reports whether it is new. Check and
// record are one step, so of concurrent identical deliveries exactly one
// wins.
func (s *seenDeliveries) claim(lockID, hash string, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	l := s.locks[lockID]
	if l == nil {
		l = &seenLock{expires: map[string]time.Time{}}
		s.locks[lockID] = l
	}
	l.prune(now, s.retention > 0)
	if _, ok := l.expires[hash]; ok {
		return false
	}
	if len(l.order) >= MaxSeenPerLock {
		delete(l.expires, l.order[0])
		l.order = l.order[1:]
	}
	l.expires[hash] = now.Add(s.retention)
	l.order = append(l.order, hash)
	return true
}

// release forgets a claimed delivery that could not be applied, so a retry
// of it is not mistaken for a repeat.
func (s *seenDeliveries) release(lockID, hash string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	l := s.locks[lockID]
	if l == nil {
		return
	}
	delete(l.expires, hash)
	for i, h := range l.order {
		if h == hash {
			l.order = append(l.order[:i], l.order[i+1:]...)
			break
		}
	}
}

func (l *seenLock) prune(now time.Time, expire bool) {
	if !expire {
		return
	}
	n := 0
	for n < len(l.order) && !now.Before(l.expires[l.order[n]]) {
		delete(l.expires, l.order[n])
		n++
	}
	l.order = l.order[n:]
}

// count reports how many deliveries are remembered for lockID.
func (s *seenDeliveries) count(lockID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	if l := s.locks[lockID]; l != nil {
		return len(l.expires)
	}
	return 0
}

// shouldLog rate-limits the repeat log line per lock; repeated is the number
// of repeats not logged since the last line.
func (s *seenDeliveries) shouldLog(lockID string, now time.Time) (ok bool, repeated int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	w := s.warned[lockID]
	if w != nil && now.Sub(w.last) < seenWarnWindow {
		w.suppressed++
		return false, 0
	}
	if w != nil {
		repeated = w.suppressed
	}
	s.warned[lockID] = &seenWarn{last: now}
	return true, repeated
}

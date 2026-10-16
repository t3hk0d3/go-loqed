package gateway

import (
	"context"
	"errors"
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/cloud"
	"github.com/t3hk0d3/go-loqed/loqed-mqtt/internal/model"
)

// enterCloud switches to cloud mode. State is stale until fresh cloud data
// (a poll or a cloud webhook) arrives.
func (s *Supervisor) enterCloud(ctx context.Context) {
	now := s.d.Now()
	s.setMode(model.ModeCloud)
	s.bridgeProbeSchedule(now)
	s.nextCloudProbe = now.Add(s.t.Liveness)
	s.nextCloudPoll = now.Add(s.t.CloudPoll)
	s.confirmAt, s.confirmViaCloud = time.Time{}, false
	s.state.StateStale = true
	s.publish()
	s.requestCloud(ctx, PriorityBackground, time.Time{})
}

func (s *Supervisor) bridgeProbeSchedule(now time.Time) {
	s.nextProbe = now.Add(s.t.Liveness)
}

func (s *Supervisor) enterOffline() {
	s.setMode(model.ModeOffline)
	s.nextOfflineRetry = s.d.Now().Add(s.t.OfflineRetry)
	s.publish()
}

func (s *Supervisor) tickCloud(ctx context.Context, now time.Time) {
	if s.canLocal() && !now.Before(s.nextProbe) {
		s.nextProbe = now.Add(s.t.Liveness)
		if s.probeBridge(ctx) && s.tryEnterLocal(ctx) {
			return
		}
	}
	if !now.Before(s.nextCloudProbe) {
		s.nextCloudProbe = now.Add(s.t.Liveness)
		if err := s.probeCloud(ctx); err != nil {
			s.cloudProbeFailures++
			s.log.Debug("cloud probe failed", "failures", s.cloudProbeFailures, "err", err)
			if s.cloudProbeFailures >= s.t.FailureThreshold {
				s.warn("LOQED cloud unreachable", "err", err)
				s.enterOffline()
				return
			}
		} else {
			s.cloudProbeFailures = 0
		}
	}
	// A reconcile-spaced poll scheduled while webhooks worked must not delay
	// polling once they stop.
	if !s.pushActive(now) && s.nextCloudPoll.After(now.Add(s.t.CloudPoll)) {
		s.nextCloudPoll = now
	}
	if !now.Before(s.nextCloudPoll) {
		s.nextCloudPoll = s.nextPollTime(now)
		s.requestCloud(ctx, PriorityBackground, time.Time{})
	}
	// A read about to land may refresh the data: check after it (applyCloudRead).
	if s.read == nil {
		s.checkFreshness(now)
	}
}

// pushActive: cloud webhooks are configured and have been seen recently.
func (s *Supervisor) pushActive(now time.Time) bool {
	return s.d.CloudWebhooks && !s.lastCloudEventAt.IsZero() && now.Sub(s.lastCloudEventAt) < s.t.Reconcile
}

// nextPollTime: with working cloud webhooks only one reconcile poll per
// Reconcile, counted from the last successful poll (events never postpone
// it); otherwise ask every CloudPoll and let the budget space the calls.
func (s *Supervisor) nextPollTime(now time.Time) time.Time {
	next := now.Add(s.t.CloudPoll)
	if s.pushActive(now) && !s.lastPollAt.IsZero() {
		if r := s.lastPollAt.Add(s.t.Reconcile); r.After(next) {
			return r
		}
	}
	return next
}

// checkFreshness marks the state stale when no fresh data arrived within
// the expected interval plus StaleGrace.
func (s *Supervisor) checkFreshness(now time.Time) {
	expected := s.t.CloudPollSpacing
	if s.pushActive(now) {
		expected = s.t.Reconcile
	}
	if now.Sub(s.lastFreshAt) > expected+s.t.StaleGrace {
		s.markStale()
	}
}

func (s *Supervisor) tickOffline(ctx context.Context, now time.Time) {
	if now.Before(s.nextOfflineRetry) {
		return
	}
	s.nextOfflineRetry = now.Add(s.t.OfflineRetry)
	if s.canLocal() && s.probeBridge(ctx) && s.tryEnterLocal(ctx) {
		return
	}
	if err := s.probeCloud(ctx); err != nil {
		s.log.Debug("cloud still unreachable", "err", err)
		return
	}
	s.enterCloud(ctx)
}

func (s *Supervisor) probeBridge(ctx context.Context) bool {
	c, cancel := s.reqCtx(ctx)
	defer cancel()
	return s.d.Probe(c, BridgeAddress(s.Record().BridgeIP)) == nil
}

func (s *Supervisor) probeCloud(ctx context.Context) error {
	if s.d.ProbeCloud == nil {
		return nil
	}
	c, cancel := s.reqCtx(ctx)
	defer cancel()
	return s.d.ProbeCloud(c)
}

// cloudRequest is a cloud read the supervisor wants.
type cloudRequest struct {
	p         Priority
	notBefore time.Time
}

// cloudRead is a finished cloud read.
type cloudRead struct {
	req  cloudRequest
	list LockList
	err  error
}

// requestCloud asks for a cloud read without waiting for it. One read is in
// flight at a time; requests meanwhile merge into the next one: a
// confirmation read replaces a background poll, and of two confirmation
// reads the later notBefore wins.
func (s *Supervisor) requestCloud(ctx context.Context, p Priority, notBefore time.Time) {
	r := cloudRequest{p: p, notBefore: notBefore}
	if s.read == nil {
		s.startRead(ctx, r)
		return
	}
	if r.p == PriorityBackground && s.readPrio == PriorityBackground {
		return // the background read in flight is this poll
	}
	switch q := s.queued; {
	case q == nil, q.p != PriorityConfirm && r.p == PriorityConfirm:
		s.queued = &r
	case q.p == PriorityConfirm && r.p == PriorityConfirm && r.notBefore.After(q.notBefore):
		q.notBefore = r.notBefore
	}
}

// startRead runs one read through Deps.Go. The result channel is buffered,
// so the read can always finish, even after the supervisor stopped.
func (s *Supervisor) startRead(ctx context.Context, r cloudRequest) {
	ch := make(chan cloudRead, 1)
	s.read, s.readPrio = ch, r.p
	s.d.Go(func() {
		list, err := s.d.Cloud.Locks(ctx, r.p, r.notBefore)
		ch <- cloudRead{req: r, list: list, err: err}
	})
}

// applyCloudRead applies a finished read on the supervisor goroutine, then
// starts the queued read, if any.
func (s *Supervisor) applyCloudRead(ctx context.Context, r cloudRead) {
	s.read = nil
	if ctx.Err() != nil {
		return
	}
	s.applyCloudResult(ctx, r.req.p, r.list, r.err)
	if q := s.queued; q != nil {
		s.queued = nil
		if q.p != PriorityBackground || s.mode != model.ModeLocal { // local mode does not poll
			s.startRead(ctx, *q)
		}
	}
	if s.mode == model.ModeCloud && s.read == nil {
		s.checkFreshness(s.d.Now())
	}
}

// applyCloudResult applies one read's outcome. It returns true on fresh data.
func (s *Supervisor) applyCloudResult(ctx context.Context, p Priority, list LockList, err error) bool {
	switch {
	case errors.Is(err, ErrDeferred):
		return false // freshness tracking marks the state stale if this lasts
	case errors.Is(err, ErrBudgetExhausted), errors.Is(err, ErrCloudBlocked), errors.Is(err, loqed.ErrRateLimited):
		s.markStale()
		return false
	case err != nil:
		s.cloudAPIFailures++
		s.warn("cloud request failed", "failures", s.cloudAPIFailures, "err", err)
		if s.mode == model.ModeCloud && s.cloudAPIFailures >= s.t.FailureThreshold {
			s.enterOffline()
		}
		return false
	}
	now := s.d.Now()
	s.cloudAPIFailures = 0
	s.lastPollAt = now
	for _, l := range list.Locks {
		if l.ID != s.id {
			continue
		}
		if s.mode == model.ModeOffline {
			s.setMode(model.ModeCloud)
			s.bridgeProbeSchedule(now)
			s.nextCloudProbe, s.nextCloudPoll = now.Add(s.t.Liveness), now.Add(s.t.CloudPoll)
		}
		// A confirmation read counts only for the confirmation it was made
		// for: one fetched before the current confirmation began is a plain
		// read (it may predate the command it would otherwise confirm).
		confirming := p == PriorityConfirm && !list.FetchedAt.Before(s.cloudConfirmSince)
		// In local mode the bridge is the source; only a confirmation read
		// (a fallback after a failed /status) may set the bolt state.
		bolt := s.mode != model.ModeLocal || confirming
		s.applyCloudLock(ctx, now, l, list.FetchedAt, confirming, bolt)
		s.publish()
		return true
	}
	s.warn("lock is missing from the cloud lock list")
	return false
}

// applyCloudLock applies polled data. Bolt data older than the last applied
// event, the last /status read or the last command request that may have
// moved the lock is ignored (a poll, which may finish late, never overwrites
// newer state or clears a "may have moved" stale mark), as is bolt data when
// bolt is false. A missing
// online field keeps the previous value. A confirmation poll that still shows
// the pre-command state (the cloud lags) is published as stale and retried
// once.
func (s *Supervisor) applyCloudLock(ctx context.Context, now time.Time, l cloud.Lock, fetchedAt time.Time, confirming, bolt bool) {
	if bolt && !fetchedAt.Before(s.lastEventAt) && !fetchedAt.Before(s.lastStatusAt) && !fetchedAt.Before(s.lastMayMoveAt) {
		s.state.BoltState = l.BoltState
		s.state.Lock = model.LockStateFor(l.BoltState)
		if confirming {
			final := s.confirmRetried || s.confirmTarget == l.BoltState
			defer s.cmds.onPoll(ctx, now, l.BoltState, final)
		}
		if confirming && s.confirmTarget != loqed.BoltUnknown && s.confirmTarget != l.BoltState {
			s.state.StateStale = true
			if !s.confirmRetried {
				s.scheduleCloudConfirm(now, s.confirmTarget)
				s.confirmRetried = true
			}
		} else {
			s.state.StateStale = false
			s.lastFreshAt = now
			if s.confirmTarget == loqed.BoltUnknown || s.confirmTarget == l.BoltState {
				s.cloudConfirmAt = time.Time{}
			}
		}
	}
	s.state.BatteryPercentage = model.Ptr(l.BatteryPercentage)
	if l.Online != nil {
		s.state.LockOnline = *l.Online
	}
}

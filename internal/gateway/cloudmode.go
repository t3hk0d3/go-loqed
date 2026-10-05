package gateway

import (
	"context"
	"errors"
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/cloud"
	"github.com/t3hk0d3/go-loqed/internal/model"
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
	s.pollCloud(ctx, PriorityBackground, time.Time{})
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
		s.pollCloud(ctx, PriorityBackground, time.Time{})
		if s.mode != model.ModeCloud {
			return
		}
	}
	s.checkFreshness(now)
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

// pollCloud reads the lock from the cloud. It returns true on fresh data.
func (s *Supervisor) pollCloud(ctx context.Context, p Priority, notBefore time.Time) bool {
	list, err := s.d.Cloud.Locks(ctx, p, notBefore)
	if ctx.Err() != nil {
		return false
	}
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
		s.applyCloudLock(now, l, list.FetchedAt)
		s.publish()
		return true
	}
	s.warn("lock is missing from the cloud lock list")
	return false
}

// applyCloudLock applies polled data. Bolt data older than the last applied
// event is ignored (a poll never overwrites newer webhook state). A missing
// online field keeps the previous value.
func (s *Supervisor) applyCloudLock(now time.Time, l cloud.Lock, fetchedAt time.Time) {
	if !fetchedAt.Before(s.lastEventAt) {
		s.state.BoltState = l.BoltState
		s.state.Lock = model.LockStateFor(l.BoltState)
		s.state.StateStale = false
		s.lastFreshAt = now
		if s.confirmTarget == loqed.BoltUnknown || s.confirmTarget == l.BoltState {
			s.cloudConfirmAt = time.Time{}
		}
	}
	s.state.BatteryPercentage = model.Ptr(l.BatteryPercentage)
	if l.Online != nil {
		s.state.LockOnline = *l.Online
	}
}

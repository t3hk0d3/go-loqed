package gateway

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/bridge"
	"github.com/t3hk0d3/go-loqed/internal/model"
)

func (s *Supervisor) canLocal() bool { return s.Record().HasLocalCredentials() }

// ensureBridge builds the bridge client if needed (no network I/O).
func (s *Supervisor) ensureBridge() bool {
	if s.bridge != nil {
		return true
	}
	if !s.canLocal() {
		return false
	}
	b, err := s.d.NewBridge(s.Record())
	if err != nil {
		s.warn("cannot create bridge client", "err", err)
		return false
	}
	s.bridge = b
	return true
}

// tryEnterLocal fetches status and, on success, switches to local mode and
// makes sure our webhook is registered.
func (s *Supervisor) tryEnterLocal(ctx context.Context) bool {
	if !s.ensureBridge() {
		return false
	}
	st, err := s.status(ctx)
	if err != nil {
		s.log.Debug("bridge not reachable", "err", err)
		return false
	}
	now := s.d.Now()
	s.setMode(model.ModeLocal)
	s.applyStatus(ctx, now, st)
	s.nextProbe = now.Add(s.t.Liveness)
	s.nextReconcile = now.Add(s.t.Reconcile)
	if !s.registerWebhook(ctx) {
		return true // registerWebhook left local mode
	}
	s.publish()
	return true
}

func (s *Supervisor) status(ctx context.Context) (*bridge.Status, error) {
	if s.bridge == nil {
		return nil, errNoBridge
	}
	c, cancel := s.reqCtx(ctx)
	defer cancel()
	return s.bridge.Status(c)
}

// registerWebhook ensures our bridge webhook. On failure it schedules a
// retry; an auth failure refreshes credentials first. It returns false if
// the lock had to leave local mode (no usable bridge client).
func (s *Supervisor) registerWebhook(ctx context.Context) bool {
	err := s.ensureWebhook(ctx)
	if errors.Is(err, loqed.ErrUnauthorized) {
		if !s.refreshAndRebuild(ctx, ReasonUnauthorized) {
			if s.bridge == nil {
				s.enterCloud(ctx)
				return false
			}
		} else {
			err = s.ensureWebhook(ctx)
		}
	}
	s.webhookOK = err == nil
	if err != nil {
		s.nextWebhookRetry = s.d.Now().Add(s.t.WebhookRetry)
		s.warn("could not register the webhook on the bridge; polling /status every 10 min until it works", "err", err)
	}
	return true
}

// ensureWebhook registers <private>/webhook/<id> and removes our stale
// registrations (same path, different host or port). Other webhooks stay.
func (s *Supervisor) ensureWebhook(ctx context.Context) error {
	if s.bridge == nil {
		return errNoBridge
	}
	want, err := s.d.WebhookURL(s.Record())
	if err != nil {
		return err
	}
	c, cancel := s.reqCtx(ctx)
	defer cancel()
	hooks, err := s.bridge.ListWebhooks(c)
	if err != nil {
		return err
	}
	suffix := "/webhook/" + s.id
	found := false
	others := 0
	for _, h := range hooks {
		if h.URL == want {
			found = true
			continue
		}
		u, perr := url.Parse(h.URL)
		if perr != nil || !strings.HasSuffix(u.Path, suffix) {
			others++
			continue
		}
		if err := s.bridge.DeleteWebhook(c, int(h.ID)); err != nil {
			s.warn("could not delete a stale webhook", "webhook_id", int(h.ID), "err", err)
		} else {
			s.log.Info("deleted a stale webhook", "webhook_id", int(h.ID))
		}
	}
	if others > maxOtherWebhooks {
		s.warn("the bridge has many other webhooks; each webhook target delays events and /status, so remove the ones no longer used",
			"other_webhooks", others)
	}
	if found {
		return nil
	}
	return s.bridge.CreateWebhook(c, want, bridge.AllTriggers)
}

func (s *Supervisor) tickLocal(ctx context.Context, now time.Time) {
	if s.bridge == nil && !s.ensureBridge() {
		s.enterCloud(ctx) // credentials became unusable
		return
	}
	steps := []func() bool{
		func() bool { // a command's or movement's webhook never arrived
			if s.confirmAt.IsZero() || now.Before(s.confirmAt) {
				return true
			}
			s.confirmAt = time.Time{}
			viaCloud := s.confirmViaCloud
			s.confirmViaCloud = false
			ok := s.reconcile(ctx)
			if !ok && viaCloud {
				s.scheduleCloudConfirm(now, s.confirmTarget)
			}
			if ok && s.move.active && !s.confirmRechecked && s.mode == model.ModeLocal {
				// Still unresolved: /status may lag; read once more.
				s.confirmRechecked = true
				s.confirmAt = s.d.Now().Add(s.t.StatusRecheck)
			}
			return s.mode == model.ModeLocal
		},
		func() bool { // webhook registration pending
			if s.webhookOK || now.Before(s.nextWebhookRetry) {
				return true
			}
			s.nextWebhookRetry = now.Add(s.t.WebhookRetry)
			if !s.registerWebhook(ctx) {
				return false
			}
			s.reconcile(ctx) // no webhooks yet: poll instead
			return s.mode == model.ModeLocal
		},
		func() bool { // TCP liveness
			if now.Before(s.nextProbe) {
				return true
			}
			s.nextProbe = now.Add(s.t.Liveness)
			s.probeLocal(ctx)
			return s.mode == model.ModeLocal
		},
		func() bool { // unknown bolt: recheck at most every 10 min
			if s.state.BoltState != loqed.BoltUnknown || now.Sub(s.lastUnknownCheck) < s.t.UnknownRecheck {
				return true
			}
			s.reconcile(ctx)
			return s.mode == model.ModeLocal
		},
		func() bool { // periodic reconcile (also retries webhook registration)
			if now.Before(s.nextReconcile) {
				return true
			}
			// Re-check the webhook every time: a bridge may drop it.
			if !s.registerWebhook(ctx) {
				return false
			}
			s.reconcile(ctx)
			return s.mode == model.ModeLocal
		},
	}
	for _, step := range steps {
		if !step() {
			return
		}
	}
}

// reconcile fetches /status; it reports whether that worked.
func (s *Supervisor) reconcile(ctx context.Context) bool {
	now := s.d.Now()
	s.nextReconcile = now.Add(s.t.Reconcile)
	if s.state.BoltState == loqed.BoltUnknown {
		s.lastUnknownCheck = now
	}
	st, err := s.status(ctx)
	if err != nil {
		s.markStale()
		s.httpFailure(ctx, err)
		return false
	}
	s.httpFailures = 0
	s.applyStatus(ctx, now, st)
	s.publish()
	return true
}

// probeLocal is the TCP liveness check. Success only resets the probe
// counter: a bridge that accepts TCP but hangs on HTTP must still fail over.
func (s *Supervisor) probeLocal(ctx context.Context) {
	c, cancel := s.reqCtx(ctx)
	defer cancel()
	if err := s.d.Probe(c, BridgeAddress(s.Record().BridgeIP)); err != nil {
		s.probeFailures++
		s.log.Debug("bridge probe failed", "failures", s.probeFailures, "err", err)
		if s.probeFailures >= s.t.FailureThreshold {
			s.localUnreachable(ctx, err)
		}
		return
	}
	s.probeFailures = 0
}

// httpFailure counts a failed bridge HTTP request.
func (s *Supervisor) httpFailure(ctx context.Context, err error) {
	if errors.Is(err, loqed.ErrUnauthorized) {
		if !s.refreshAndRebuild(ctx, ReasonUnauthorized) && s.bridge == nil {
			s.enterCloud(ctx)
		}
		return
	}
	if s.countHTTPFailure(err) {
		s.localUnreachable(ctx, err)
	}
}

// countHTTPFailure counts a failed bridge HTTP request and reports whether
// the failover threshold is reached, leaving the failover to the caller.
func (s *Supervisor) countHTTPFailure(err error) bool {
	s.httpFailures++
	s.log.Debug("bridge request failed", "failures", s.httpFailures, "err", err)
	return s.httpFailures >= s.t.FailureThreshold
}

// localUnreachable tries a credential refresh (the IP may have changed,
// unless pinned in lock_settings) and otherwise falls back to cloud mode.
func (s *Supervisor) localUnreachable(ctx context.Context, err error) {
	s.warn("bridge unreachable", "err", err)
	oldIP := s.Record().BridgeIP
	if s.refreshAndRebuild(ctx, ReasonUnreachable) && s.Record().BridgeIP != oldIP {
		s.log.Info("bridge IP changed", "old", oldIP, "new", s.Record().BridgeIP)
		if s.tryEnterLocal(ctx) {
			return
		}
	}
	s.enterCloud(ctx)
}

// refreshAndRebuild reloads credentials from the cloud. It is skipped when
// lock_settings pins what the refresh would change. On success the bridge
// client is rebuilt; if that fails, s.bridge stays nil.
func (s *Supervisor) refreshAndRebuild(ctx context.Context, reason Reason) bool {
	switch {
	case reason == ReasonUnauthorized && KeysPinned(s.setting):
		s.warn("the bridge rejected the keys pinned in lock_settings; fix bridge_key/key_secret/local_id")
		return false
	case reason == ReasonUnreachable && IPPinned(s.setting):
		return false
	}
	rec, err := s.d.Refresh(ctx, s.id, reason)
	if err != nil {
		if !errors.Is(err, ErrRefreshThrottled) {
			s.warn("credential refresh failed", "reason", reason, "err", err)
		}
		return false
	}
	s.setRecord(rec)
	return s.ensureBridge()
}

func (s *Supervisor) onBridgeEvent(ctx context.Context, ev bridge.Event) {
	// The event is signed with the bridge key, so it is authentic even if
	// entering local mode fails; apply it either way.
	if s.mode != model.ModeLocal {
		s.tryEnterLocal(ctx)
	}
	now := s.d.Now()
	if s.mode == model.ModeLocal {
		s.probeFailures, s.httpFailures = 0, 0
		s.nextProbe = now.Add(s.t.Liveness) // a webhook proves the bridge is alive
	}
	switch e := ev.(type) {
	case bridge.StateReachedEvent:
		if s.isDuplicate("bridge", e.EventType, e.KeyLocalID, now) {
			return
		}
		s.state.LockOnline = true
		if !e.Jammed {
			s.move = movement{}
		}
		s.onReached(now, e.BoltState, e.Jammed)
		s.recordEvent(now, e.EventType, e.KeyLocalID, "", model.FromStateReached(e.EventType))
		s.cmds.onReached(ctx, now, e.BoltState, e.Jammed, e.KeyLocalID)
	case bridge.GoToStateEvent:
		if s.isDuplicate("bridge", e.EventType, e.KeyLocalID, now) {
			return
		}
		s.startMovement(now, e.GoToState)
		s.recordEvent(now, e.EventType, e.KeyLocalID, "", model.FromGoTo(e.GoToState, s.state.Lock))
		s.cmds.onGoTo(now, e.GoToState, e.KeyLocalID)
		if s.confirmAt.IsZero() {
			s.awaitConfirm(now, e.GoToState) // STATE_CHANGED may be lost
		}
	case bridge.BatteryEvent:
		if e.BatteryPercentage >= 0 {
			s.state.BatteryPercentage = model.Ptr(e.BatteryPercentage)
		}
		if e.WifiStrength != nil {
			s.state.WifiStrength = e.WifiStrength
		}
		if e.BLEStrength != nil {
			s.state.BLEStrength = e.BLEStrength
			s.state.LockOnline = *e.BLEStrength != -1
		} else {
			s.state.LockOnline = e.BatteryPercentage != -1
		}
		s.publish()
	case bridge.OnlineEvent:
		if e.WifiStrength != nil {
			s.state.WifiStrength = e.WifiStrength
		}
		if e.BLEStrength != nil {
			s.state.BLEStrength = e.BLEStrength
		}
		s.state.LockOnline = e.BLEStrength == nil || *e.BLEStrength != -1
		s.publish()
	}
}

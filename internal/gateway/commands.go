package gateway

import (
	"context"
	"errors"
	"fmt"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/bridge"
	"github.com/t3hk0d3/go-loqed/internal/model"
)

func actionFor(c model.Command) bridge.Action {
	switch c {
	case model.CommandOpen:
		return bridge.ActionOpen
	case model.CommandUnlock:
		return bridge.ActionUnlock
	default:
		return bridge.ActionLock
	}
}

// onCommand executes LOCK/UNLOCK/OPEN. Every actuation runs under an
// absolute deadline of At+CommandMaxAge, so a command is never executed
// late. It is sent through the cloud only when the bridge provably did not
// receive it; anything the bridge may have acted on is verified, never
// resent. Refreshes triggered by a failure run after the command resolved.
func (s *Supervisor) onCommand(ctx context.Context, m CommandMsg) {
	now := s.d.Now()
	deadline := m.At.Add(s.t.CommandMaxAge)
	if !now.Before(deadline) {
		s.commandFailed(m.Command, model.FailExpired, fmt.Errorf("command older than %s when dequeued", s.t.CommandMaxAge))
		return
	}
	// The deadline is computed on the supervisor clock; contexts expire on
	// the real clock, so pass the remaining time.
	cctx, cancel := context.WithTimeout(ctx, deadline.Sub(now))
	defer cancel()
	switch s.mode {
	case model.ModeOffline:
		s.commandFailed(m.Command, model.FailOffline, errors.New("the lock is offline"))
	case model.ModeCloud:
		s.sendViaCloud(cctx, m.Command)
	default:
		s.sendViaBridge(ctx, cctx, m.Command)
	}
}

func (s *Supervisor) sendViaBridge(ctx, cctx context.Context, c model.Command) {
	now := s.d.Now()
	err := s.bridgeCommand(cctx, c)
	switch {
	case err == nil:
		s.httpFailures = 0
		s.awaitConfirm(now, c.Target())
	case errors.Is(err, loqed.ErrUnreachable), errors.Is(err, errNoBridge):
		// Never delivered: the cloud may send it.
		s.log.Warn("bridge did not receive the command; sending it via the cloud", "command", c, "err", err)
		s.sendViaCloud(cctx, c)
		if !errors.Is(err, errNoBridge) {
			s.httpFailure(ctx, err)
		}
	case errors.Is(err, loqed.ErrUnauthorized):
		// The bridge rejected the signature, so nothing happened.
		s.log.Warn("bridge rejected the command signature; sending it via the cloud", "command", c)
		s.sendViaCloud(cctx, c)
		if !s.refreshAndRebuild(ctx, ReasonUnauthorized) && s.bridge == nil {
			s.enterCloud(ctx)
		}
	default:
		// The bridge may have acted (timeout after sending, reset, 5xx):
		// do not resend; check the outcome right away.
		s.commandFailed(c, failClass(err), err)
		s.awaitConfirm(now, c.Target())
		s.confirmAt, s.confirmViaCloud = now, true
		s.httpFailure(ctx, err)
		if s.mode != model.ModeLocal {
			// Failover cleared the pending confirmation; the uncertain
			// command must still be verified.
			s.scheduleCloudConfirm(s.d.Now(), c.Target())
		}
	}
}

func (s *Supervisor) bridgeCommand(ctx context.Context, c model.Command) error {
	if s.bridge == nil && !s.ensureBridge() {
		return errNoBridge
	}
	rc, cancel := s.reqCtx(ctx)
	defer cancel()
	return s.bridge.Command(rc, actionFor(c))
}

// sendViaCloud sends the command through the cloud within the command's
// deadline and schedules one confirmation poll (also when the outcome is
// unknown: the command may have run).
func (s *Supervisor) sendViaCloud(cctx context.Context, c model.Command) {
	if err := cctx.Err(); err != nil {
		s.commandFailed(c, model.FailExpired, err)
		return
	}
	err := s.d.Cloud.Command(cctx, s.id, c.Target())
	now := s.d.Now()
	if err == nil {
		moving := c.Moving()
		s.state.Lock = &moving
		s.publish()
		s.scheduleCloudConfirm(now, c.Target())
		return
	}
	class := failClass(err)
	s.commandFailed(c, class, err)
	if class == model.FailNoResponse {
		s.scheduleCloudConfirm(now, c.Target())
	}
}

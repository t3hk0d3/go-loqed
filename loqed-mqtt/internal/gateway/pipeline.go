package gateway

import (
	"context"
	"errors"
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/bridge"
	"github.com/t3hk0d3/go-loqed/cloud"
	"github.com/t3hk0d3/go-loqed/loqed-mqtt/internal/model"
)

// localBackoff is the wait after the n-th undelivered local attempt; later
// attempts repeat the last value.
var localBackoff = []time.Duration{500 * time.Millisecond, time.Second, 2 * time.Second}

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

// command is one LOCK/UNLOCK/OPEN on its way to the lock.
type command struct {
	cmd        model.Command
	id         string
	receivedAt time.Time
	deadline   time.Time // no attempt starts after this
	cutoff     time.Time // no local attempt starts after this

	status   model.CommandStatusValue
	via      model.Via
	attempts int
	errClass string

	localAttempts int
	nextAttempt   time.Time
	skipLocal     bool // the bridge cannot be used for this command
	cloudTried    bool
	lastLocalErr  error
	refreshAfter  bool // refresh the lock's credentials once it resolves
	failoverAfter bool // its local failures reached the threshold: fail over once it resolves

	from      loqed.BoltState // bolt state when the first request went out
	written   bool            // a request may have reached the bridge or cloud
	sentAt    time.Time       // 2xx from the bridge or cloud
	confirmBy time.Time
	settleAt  time.Time // accepted while already in the target state
}

func (c *command) target() loqed.BoltState { return c.cmd.Target() }

// terminal: nothing more happens to the command except a late confirmation.
func (c *command) terminal() bool {
	switch c.status {
	case model.StatusConfirmed, model.StatusFailed, model.StatusExpired, model.StatusSuperseded:
		return true
	}
	return false
}

// inFlight: written and waiting for its confirmation.
func (c *command) inFlight() bool {
	return c.written && (c.status == model.StatusSent || c.status == model.StatusAccepted)
}

// commandPipeline delivers one lock's commands (spec 5.8). Latest command
// wins: one pending slot; a command is retried only while it provably never
// left, and never once a request may have been written. It lives on the
// supervisor goroutine.
type commandPipeline struct {
	s       *Supervisor
	active  *command // being delivered, or written and awaiting confirmation
	pending *command // waits for the in-flight active command
	watch   *command // last written command; a late confirmation still counts
	busy    bool     // step is running (it can be re-entered via failover and /status)
}

// Submit accepts a command that arrived over MQTT at receivedAt.
func (p *commandPipeline) Submit(ctx context.Context, now time.Time, c model.Command, id string, receivedAt time.Time) {
	s := p.s
	deadline, cutoffGap := receivedAt.Add(s.t.CommandDeadline), s.t.LocalCutoff
	if c == model.CommandOpen {
		deadline, cutoffGap = receivedAt.Add(s.t.OpenDeadline), s.t.OpenLocalCutoff
	}
	nc := &command{cmd: c, id: id, receivedAt: receivedAt, deadline: deadline, cutoff: deadline.Add(-cutoffGap),
		status: model.StatusPending}
	if s.mode == model.ModeOffline {
		p.fail(ctx, now, nc, model.FailOffline, errors.New("the lock is offline"))
		return
	}
	latest := p.pending
	if latest == nil && p.active != nil && !p.active.terminal() {
		latest = p.active
	}
	if latest != nil && latest.cmd == c {
		p.coalesce(now, latest, id)
		return
	}
	if p.pending != nil {
		p.supersede(now, p.pending)
		p.pending = nil
	}
	if p.active != nil && !p.active.written && !p.active.terminal() {
		p.supersede(now, p.active)
		p.active = nil
	}
	if p.active != nil && !p.active.terminal() && p.active.cmd == c {
		p.coalesce(now, p.active, id)
		return
	}
	p.publish(now, nc)
	if p.active != nil && !p.active.terminal() {
		p.pending = nc
		return
	}
	p.active = nc
	p.step(ctx, now)
}

func (p *commandPipeline) coalesce(now time.Time, c *command, id string) {
	if id != "" {
		c.id = id
	}
	p.publish(now, c)
}

func (p *commandPipeline) supersede(now time.Time, c *command) {
	c.status = model.StatusSuperseded
	p.publish(now, c)
	p.s.log.Info("lock command superseded by a newer one", "command", c.cmd)
}

// nextWake is when the next local retry is due (zero if none), so the
// supervisor can run sub-second retries between its 1 s ticks.
func (p *commandPipeline) nextWake() time.Time {
	if a := p.active; a != nil && !a.written && !a.terminal() {
		return a.nextAttempt
	}
	return time.Time{}
}

// step advances timers and delivery; called after Submit, on every tick
// and when nextWake is due.
func (p *commandPipeline) step(ctx context.Context, now time.Time) {
	if p.busy {
		return
	}
	p.busy = true
	defer func() { p.busy = false }()
	if a := p.active; a != nil && a.inFlight() {
		switch {
		case !a.settleAt.IsZero() && !now.Before(a.settleAt):
			p.confirm(ctx, now, a)
		case !now.Before(a.confirmBy):
			p.noConfirmation(ctx, now, a)
		}
	}
	if p.active != nil && p.active.terminal() {
		p.active = nil
	}
	if p.active == nil && p.pending != nil {
		p.active, p.pending = p.pending, nil
	}
	if pd := p.pending; pd != nil && !now.Before(pd.deadline) {
		p.pending = nil
		p.fail(ctx, now, pd, model.FailExpired, errors.New("deadline passed while an earlier command was in flight"))
	}
	a := p.active
	if a == nil || a.written || a.terminal() || now.Before(a.nextAttempt) {
		return
	}
	if !now.Before(a.deadline) {
		p.fail(ctx, now, a, model.FailExpired, errors.New("deadline passed before the command could be sent"))
		return
	}
	p.attempt(ctx, now, a)
}

func (p *commandPipeline) attempt(ctx context.Context, now time.Time, a *command) {
	s := p.s
	if a.attempts == 0 {
		a.from = s.state.BoltState
	}
	switch {
	case s.mode == model.ModeOffline:
		p.fail(ctx, now, a, model.FailOffline, errors.New("the lock is offline"))
		return
	case s.mode == model.ModeLocal && !a.skipLocal && now.Before(a.cutoff):
		if s.bridge == nil && !s.ensureBridge() {
			a.skipLocal = true
			break
		}
		if !p.attemptLocal(ctx, now, a) {
			return
		}
	}
	if a.terminal() || a.written {
		return
	}
	p.attemptCloud(ctx, s.d.Now(), a)
}

// attemptLocal sends one signed bridge command (a fresh signature every
// time: the lock rejects stale timestamps). It reports whether delivery
// should continue via the cloud right away.
func (p *commandPipeline) attemptLocal(ctx context.Context, now time.Time, a *command) bool {
	s := p.s
	a.attempts++
	a.localAttempts++
	a.status = model.StatusSending
	p.publish(now, a)
	// End by the local cutoff: a bridge that hangs must leave the cloud
	// attempt its time.
	rc, cancel := context.WithTimeout(ctx, min(s.t.RequestTimeout, a.cutoff.Sub(now)))
	err := s.bridge.Command(rc, actionFor(a.cmd))
	cancel()
	after := s.d.Now()
	switch {
	case err == nil:
		s.httpFailures = 0
		p.sent(after, a, model.ViaLocal)
		return false
	case errors.Is(err, loqed.ErrUnreachable):
		// Provably never left: retry locally while time allows.
		a.lastLocalErr = err
		delay := localBackoff[min(a.localAttempts, len(localBackoff))-1]
		if next := after.Add(delay); next.Before(a.cutoff) {
			a.nextAttempt = next
			s.log.Debug("bridge did not receive the command; retrying", "command", a.cmd, "attempt", a.localAttempts, "err", err)
			return false
		}
		s.warn("bridge did not receive the command; sending it via the cloud", "command", a.cmd, "attempts", a.localAttempts, "err", err)
		// Once per command, not per attempt. A failover waits until the
		// command resolves: its cloud attempt comes first.
		a.failoverAfter = s.countHTTPFailure(err)
		return true
	case errors.Is(err, loqed.ErrUnauthorized):
		// The bridge rejected the signature, so nothing happened.
		s.warn("bridge rejected the command signature; sending it via the cloud", "command", a.cmd)
		a.lastLocalErr, a.skipLocal, a.refreshAfter = err, true, true
		return true
	default:
		// The bridge may have acted (timeout after sending, reset, error
		// status): never resend; the confirmation tells what happened.
		a.written = true
		p.fail(ctx, after, a, failClass(err), err)
		p.watchFor(after, a)
		s.awaitConfirm(after, a.target())
		s.confirmViaCloud = true
		s.httpFailure(ctx, err)
		if s.mode != model.ModeLocal {
			s.scheduleCloudConfirm(s.d.Now(), a.target())
		}
		return false
	}
}

// attemptCloud sends the command's one cloud request.
func (p *commandPipeline) attemptCloud(ctx context.Context, now time.Time, a *command) {
	s := p.s
	if a.cloudTried {
		p.fail(ctx, now, a, model.FailUnreachable, a.lastLocalErr)
		return
	}
	if !now.Before(a.deadline) {
		p.fail(ctx, now, a, model.FailExpired, errors.New("deadline passed before the command could be sent"))
		return
	}
	a.cloudTried = true
	a.attempts++
	a.status = model.StatusSending
	p.publish(now, a)
	cctx, cancel := context.WithTimeout(ctx, a.deadline.Sub(now))
	err := s.d.Cloud.Command(cctx, s.id, a.target())
	cancel()
	after := s.d.Now()
	switch class := failClass(err); {
	case err == nil:
		p.sent(after, a, model.ViaCloud)
	case class == model.FailNoResponse:
		// The cloud may have acted: never resend, confirm instead.
		a.written, a.via = true, model.ViaCloud
		p.fail(ctx, after, a, class, err)
		p.watchFor(after, a)
		s.scheduleCloudConfirm(after, a.target())
	default:
		if class == model.FailKeyDeleted {
			a.refreshAfter = true // the cached local key is dead too
		}
		p.fail(ctx, after, a, class, err)
	}
}

// sent records a 2xx: delivered, not yet confirmed (the bridge answers 200
// even for a wrong signature).
func (p *commandPipeline) sent(now time.Time, a *command, via model.Via) {
	s := p.s
	a.written, a.via, a.status = true, via, model.StatusSent
	a.sentAt, a.confirmBy = now, now.Add(s.t.WebhookConfirm)
	p.watchFor(now, a)
	s.lastCommandSentAt = now
	s.startMovement(now, a.target())
	if want := model.LockStateFor(a.target()); s.state.Lock == nil || want == nil || *s.state.Lock != *want {
		moving := a.cmd.Moving()
		s.state.Lock = &moving
	}
	// Poll data fetched before the command must not overwrite this.
	s.lastEventAt = now
	if via == model.ViaLocal {
		s.awaitConfirm(now, a.target())
		s.bridgeCheckSince, s.bridgeCheckAt, s.bridgeCheckCmd = now, now.Add(s.t.WebhookConfirm), a
	} else {
		s.scheduleCloudConfirm(now, a.target())
	}
	p.publish(now, a)
	s.publish()
}

func (p *commandPipeline) watchFor(now time.Time, a *command) {
	p.watch = a
	a.sentAt = now
}

// onGoTo: the lock started moving. Our key moving toward our target means
// the lock accepted the command.
func (p *commandPipeline) onGoTo(now time.Time, target loqed.BoltState, key *int) {
	a := p.active
	if a == nil || a.status != model.StatusSent || target != a.target() || !p.s.isGatewayKey(key) {
		return
	}
	a.status = model.StatusAccepted
	if p.s.state.BoltState == target {
		// Already there: the lock will not send STATE_CHANGED.
		a.settleAt = now.Add(p.s.t.AlreadyThere)
	}
	p.publish(now, a)
}

// onReached: STATE_CHANGED_* or MOTOR_STALL.
func (p *commandPipeline) onReached(ctx context.Context, now time.Time, bolt loqed.BoltState, jammed bool, key *int) {
	defer p.step(ctx, now) // a resolved command lets the pending one go
	if a := p.active; a != nil && a.inFlight() {
		switch {
		case jammed && p.s.isGatewayKey(key):
			p.fail(ctx, now, a, model.FailStalled, errors.New("the motor stalled"))
		case !jammed && bolt == a.target():
			p.confirm(ctx, now, a)
		case !jammed && !a.settleAt.IsZero():
			a.settleAt = time.Time{} // it moved after all; wait for the target
		}
		return
	}
	p.lateConfirm(ctx, now, bolt, jammed)
}

// showsChange: a read showing the target proves the command only if the
// lock was elsewhere when it was sent (a lock that ignored the command, for
// example after its key was deleted, still reads as the old state).
func (c *command) showsChange(bolt loqed.BoltState) bool {
	return bolt == c.target() && c.from != c.target()
}

// onStatus: a successful /status read that showed bolt.
func (p *commandPipeline) onStatus(ctx context.Context, now time.Time, bolt loqed.BoltState) {
	defer p.step(ctx, now)
	if a := p.active; a != nil && a.inFlight() {
		if a.showsChange(bolt) {
			p.confirm(ctx, now, a)
		}
		return
	}
	p.lateConfirm(ctx, now, bolt, false)
}

// onPoll: a confirmation poll fetched after the command. final means no
// further confirmation poll follows.
func (p *commandPipeline) onPoll(ctx context.Context, now time.Time, bolt loqed.BoltState, final bool) {
	defer p.step(ctx, now)
	a := p.active
	if a == nil || !a.inFlight() {
		p.lateConfirm(ctx, now, bolt, false)
		return
	}
	switch {
	case a.showsChange(bolt):
		p.confirm(ctx, now, a)
	case final:
		p.noConfirmation(ctx, now, a)
	}
}

// lateConfirm marks the last written command confirmed when its target is
// reached after it was reported failed with an unknown outcome (no response,
// no confirmation in time). A rejected command stays failed: a later move
// to the same state is someone else's.
func (p *commandPipeline) lateConfirm(ctx context.Context, now time.Time, bolt loqed.BoltState, jammed bool) {
	w := p.watch
	if w == nil || jammed || !w.showsChange(bolt) || w.status != model.StatusFailed || now.Sub(w.sentAt) > p.s.t.StatusMoveWindow ||
		(w.errClass != model.FailNoResponse && w.errClass != model.FailNoConfirmation) {
		return
	}
	p.watch = nil
	w.errClass = ""
	p.confirm(ctx, now, w)
}

func (p *commandPipeline) confirm(ctx context.Context, now time.Time, a *command) {
	a.status, a.settleAt = model.StatusConfirmed, time.Time{}
	if p.watch == a {
		p.watch = nil
	}
	p.publish(now, a)
	p.resolved(ctx, a)
}

// noConfirmation: nothing showed the target in time. The lock state can no
// longer be trusted.
func (p *commandPipeline) noConfirmation(ctx context.Context, now time.Time, a *command) {
	s := p.s
	if l := s.state.Lock; l != nil && *l == a.cmd.Moving() {
		s.state.Lock = model.LockStateFor(s.state.BoltState)
	}
	s.state.StateStale = true
	s.publish()
	p.fail(ctx, now, a, model.FailNoConfirmation, errors.New("no webhook or /status showed the target state"))
}

// fail ends a command: failed (with an error class) or expired.
func (p *commandPipeline) fail(ctx context.Context, now time.Time, a *command, class string, err error) {
	a.settleAt = time.Time{}
	if class == model.FailExpired {
		a.status, a.errClass = model.StatusExpired, ""
	} else {
		a.status, a.errClass = model.StatusFailed, class
	}
	p.publish(now, a)
	p.s.commandFailed(a.cmd, class, err)
	p.resolved(ctx, a)
}

// resolved runs what had to wait for the command to finish.
func (p *commandPipeline) resolved(ctx context.Context, a *command) {
	if a.refreshAfter {
		a.refreshAfter = false
		if !p.s.refreshAndRebuild(ctx, ReasonUnauthorized) && p.s.bridge == nil && p.s.mode == model.ModeLocal {
			p.s.enterCloud(ctx)
		}
	}
	if a.failoverAfter {
		a.failoverAfter = false
		// A bridge webhook meanwhile reset the count: the bridge is alive.
		if p.s.mode == model.ModeLocal && p.s.httpFailures >= p.s.t.FailureThreshold {
			p.s.localUnreachable(ctx, a.lastLocalErr)
		}
	}
}

func (p *commandPipeline) publish(now time.Time, a *command) {
	st := model.CommandStatus{Command: a.cmd, Status: a.status, Attempts: a.attempts, ReceivedAt: a.receivedAt, UpdatedAt: now}
	if a.id != "" {
		st.ID = model.Ptr(a.id)
	}
	if a.via != "" {
		st.Via = model.Ptr(a.via)
	}
	if a.errClass != "" {
		st.Error = model.Ptr(a.errClass)
	}
	if err := p.s.d.Publisher.PublishCommandStatus(p.s.id, st); err != nil {
		p.s.log.Warn("publishing command status failed", "err", err)
	}
}

// ErrNoCloudAccess: no cloud request could be made (no usable token).
var ErrNoCloudAccess = errors.New("gateway: no cloud access")

// failClass maps an error onto a command failure class.
func failClass(err error) string {
	switch {
	case errors.Is(err, cloud.ErrKeyDeleted):
		return model.FailKeyDeleted
	case errors.Is(err, loqed.ErrNoResponse), loqed.IsServerError(err):
		return model.FailNoResponse
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return model.FailExpired
	case errors.Is(err, loqed.ErrUnreachable), errors.Is(err, errNoBridge), errors.Is(err, ErrNoCloudAccess):
		return model.FailUnreachable
	case errors.Is(err, loqed.ErrUnauthorized):
		return model.FailUnauthorized
	case errors.Is(err, loqed.ErrRateLimited):
		return model.FailRateLimited
	default:
		return model.FailRejected
	}
}

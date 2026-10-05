package gateway

import (
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/bridge"
	"github.com/t3hk0d3/go-loqed/internal/model"
)

// movement is a bolt movement the gateway expects: started by a gateway
// command or announced by GO_TO_STATE_*, ended by any STATE_CHANGED_*.
type movement struct {
	active bool
	from   loqed.BoltState
	target loqed.BoltState
	at     time.Time
}

func (s *Supervisor) startMovement(now time.Time, target loqed.BoltState) {
	s.move = movement{active: true, from: s.state.BoltState, target: target, at: now}
}

// applyStatus applies a successful /status read. Battery and signal always
// apply; the bolt only as a hint (see applyStatusHint).
func (s *Supervisor) applyStatus(now time.Time, st *bridge.Status) bool {
	s.state.BatteryPercentage = model.Ptr(int(st.BatteryPercentage))
	s.state.BatteryVoltage = model.Ptr(float64(st.BatteryVoltage))
	s.state.WifiStrength = model.Ptr(int(st.WifiStrength))
	s.state.BLEStrength = model.Ptr(int(st.BLEStrength))
	s.state.LockOnline = st.LockOnline == 1
	if st.BoltState == loqed.BoltUnknown {
		s.lastUnknownCheck = now
	}
	return s.applyStatusHint(now, st.BoltState)
}

// applyStatusHint applies the /status bolt state only when it cannot undo
// newer webhook state: the bridge updates /status after delivering its
// webhooks, which can take minutes. It reports whether the read showed the
// target of a pending movement.
func (s *Supervisor) applyStatusHint(now time.Time, bolt loqed.BoltState) (matchedTarget bool) {
	if s.move.active && now.Sub(s.move.at) >= s.t.StatusMoveWindow {
		s.move = movement{}
	}
	if s.move.active {
		switch bolt {
		case s.move.target:
			s.move = movement{}
			s.setBoltFromStatus(now, bolt)
			return true
		case s.move.from:
			// Inconclusive: the movement may not be reflected yet.
			s.state.StateStale = true
			return false
		}
	}
	webhookRecent := !s.lastBoltEventAt.IsZero() && now.Sub(s.lastBoltEventAt) < s.t.StatusEventWindow
	if !webhookRecent || s.state.BoltState == loqed.BoltUnknown {
		s.setBoltFromStatus(now, bolt)
		return false
	}
	s.state.StateStale = false // a fresh read and a recent webhook: nothing is stale
	s.lastFreshAt = now
	return false
}

func (s *Supervisor) setBoltFromStatus(now time.Time, bolt loqed.BoltState) {
	s.state.BoltState = bolt
	s.state.Lock = model.LockStateFor(bolt)
	s.state.StateStale = false
	s.lastFreshAt = now
}

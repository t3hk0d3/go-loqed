package model

import (
	"strings"

	loqed "github.com/t3hk0d3/go-loqed"
)

// Transition is the effect of one LOQED event on State.
type Transition struct {
	SetLock bool
	Lock    *LockState // nil with SetLock = unknown
	SetBolt bool
	Bolt    loqed.BoltState
	Event   EventType
}

func (s *State) Apply(t Transition) {
	if t.SetLock {
		s.Lock = t.Lock
	}
	if t.SetBolt {
		s.BoltState = t.Bolt
	}
}

// LockStateFor maps a bolt position; unknown maps to nil (HA "unknown",
// matching HA core since 2026-10-01).
func LockStateFor(b loqed.BoltState) *LockState {
	switch b {
	case loqed.BoltNightLock:
		return Ptr(Locked)
	case loqed.BoltDayLock:
		return Ptr(Unlocked)
	case loqed.BoltOpen:
		return Ptr(Open)
	default:
		return nil
	}
}

// FromStateReached handles STATE_CHANGED_* (incl. *_REMOTE) and MOTOR_STALL.
// The bolt state comes from the event type (loqed.ReachedState), never from
// requested_state. Any other event type is unrecognized and leaves the state
// untouched (EventUnknown only); STATE_CHANGED_UNKNOWN still sets unknown.
func FromStateReached(eventType string) Transition {
	et := strings.TrimSuffix(strings.ToUpper(strings.TrimSpace(eventType)), "_REMOTE")
	if !strings.HasPrefix(et, "STATE_CHANGED_") && et != "MOTOR_STALL" {
		return Transition{Event: EventUnknown}
	}
	b, jammed := loqed.ReachedState(eventType)
	if jammed {
		return Transition{SetLock: true, Lock: Ptr(Jammed), Event: EventJammed}
	}
	t := Transition{SetLock: true, Lock: LockStateFor(b), SetBolt: true, Bolt: b}
	switch b {
	case loqed.BoltNightLock:
		t.Event = EventLocked
	case loqed.BoltDayLock:
		t.Event = EventUnlocked
	case loqed.BoltOpen:
		t.Event = EventOpened
	default:
		t.Event = EventUnknown
	}
	return t
}

// FromGoTo handles GO_TO_STATE_* events. The lock only shows a moving
// state when the target differs from the current state.
func FromGoTo(target loqed.BoltState, current *LockState) Transition {
	var want, moving LockState
	var ev EventType
	switch target {
	case loqed.BoltNightLock:
		want, moving, ev = Locked, Locking, EventLocking
	case loqed.BoltDayLock:
		want, moving, ev = Unlocked, Unlocking, EventUnlocking
	case loqed.BoltOpen:
		want, moving, ev = Open, Opening, EventOpening
	default:
		return Transition{Event: EventUnknown}
	}
	t := Transition{Event: ev}
	if current == nil || *current != want {
		t.SetLock, t.Lock = true, Ptr(moving)
	}
	return t
}

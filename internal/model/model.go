// Package model holds the per-lock state document published to MQTT and
// the mapping from LOQED events to Home Assistant states and events.
package model

import (
	"strings"
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
)

type Mode string

const (
	ModeLocal   Mode = "local"
	ModeCloud   Mode = "cloud"
	ModeOffline Mode = "offline"
)

// LockState values match the HA MQTT lock's state_* payloads.
type LockState string

const (
	Locked    LockState = "LOCKED"
	Unlocked  LockState = "UNLOCKED"
	Open      LockState = "OPEN"
	Locking   LockState = "LOCKING"
	Unlocking LockState = "UNLOCKING"
	Opening   LockState = "OPENING"
	Jammed    LockState = "JAMMED"
)

// EventType values are the HA event entity's event_types.
type EventType string

const (
	EventLocked    EventType = "locked"
	EventUnlocked  EventType = "unlocked"
	EventOpened    EventType = "opened"
	EventLocking   EventType = "locking"
	EventUnlocking EventType = "unlocking"
	EventOpening   EventType = "opening"
	EventJammed    EventType = "jammed"
	EventUnknown   EventType = "unknown"
	// EventCommandFailed is emitted by the gateway when a LOCK/UNLOCK/OPEN
	// command could not be executed (or its outcome is uncertain).
	EventCommandFailed EventType = "command_failed"
)

var EventTypes = []EventType{EventLocked, EventUnlocked, EventOpened, EventLocking, EventUnlocking, EventOpening, EventJammed, EventUnknown, EventCommandFailed}

// SourceGateway marks events produced by the gateway itself.
const SourceGateway = "gateway"

// Command failure classes published in Event.Error.
const (
	FailExpired      = "expired"      // older than CommandMaxAge before it could be sent
	FailOffline      = "offline"      // no path to the lock
	FailUnreachable  = "unreachable"  // not delivered
	FailNoResponse   = "no_response"  // maybe delivered; outcome being verified
	FailUnauthorized = "unauthorized" // credentials rejected
	FailRateLimited  = "rate_limited" // cloud rate limit
	FailOther        = "failed"
)

// State is the retained JSON document on <base>/<id>/state.
type State struct {
	Lock              *LockState      `json:"lock"`
	BoltState         loqed.BoltState `json:"bolt_state"`
	BatteryPercentage *int            `json:"battery_percentage"`
	BatteryVoltage    *float64        `json:"battery_voltage"`
	WifiStrength      *int            `json:"wifi_strength"`
	BLEStrength       *int            `json:"ble_strength"`
	LockOnline        bool            `json:"lock_online"`
	Mode              Mode            `json:"mode"`
	LastEvent         string          `json:"last_event"`
	LastKeyID         *int            `json:"last_key_id"`
	LastKeyName       *string         `json:"last_key_name"`
	LastEventAt       *time.Time      `json:"last_event_at"`
	StateStale        bool            `json:"state_stale"`
}

// Event is the non-retained JSON on <base>/<id>/event.
type Event struct {
	EventType  EventType `json:"event_type"`
	Reason     string    `json:"reason"`
	Source     string    `json:"source"`
	KeyLocalID *int      `json:"key_local_id"`
	KeyName    *string   `json:"key_name"`
	Error      string    `json:"error,omitempty"` // command_failed only
}

type Command string

const (
	CommandLock   Command = "LOCK"
	CommandUnlock Command = "UNLOCK"
	CommandOpen   Command = "OPEN"
)

func ParseCommand(s string) (Command, bool) {
	switch c := Command(strings.ToUpper(strings.TrimSpace(s))); c {
	case CommandLock, CommandUnlock, CommandOpen:
		return c, true
	default:
		return "", false
	}
}

func (c Command) Target() loqed.BoltState {
	switch c {
	case CommandOpen:
		return loqed.BoltOpen
	case CommandUnlock:
		return loqed.BoltDayLock
	default:
		return loqed.BoltNightLock
	}
}

func (c Command) Moving() LockState {
	switch c {
	case CommandOpen:
		return Opening
	case CommandUnlock:
		return Unlocking
	default:
		return Locking
	}
}

func Ptr[T any](v T) *T { return &v }

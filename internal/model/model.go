// Package model holds the per-lock state document published to MQTT and
// the mapping from LOQED events to Home Assistant states and events.
package model

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"

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

// SourceGateway marks events caused by the gateway: its own key acting on
// one of its commands, and command_failed. Every other event has source
// null; who or what acted is in the raw event type (reason) and the key.
const SourceGateway = "gateway"

// Command failure classes, published as command_status.error and as the
// command_failed event's error.
const (
	FailUnreachable    = "unreachable"     // never delivered anywhere
	FailNoResponse     = "no_response"     // maybe delivered; confirmation still runs
	FailRejected       = "rejected"        // answered with an error
	FailUnauthorized   = "unauthorized"    // credentials rejected
	FailKeyDeleted     = "key_deleted"     // the token's lock key was deleted in the LOQED app
	FailRateLimited    = "rate_limited"    // cloud rate limit
	FailStalled        = "stalled"         // MOTOR_STALL
	FailNoConfirmation = "no_confirmation" // no webhook or status showed the target in time
	FailOffline        = "offline"         // no path to the lock
	FailExpired        = "expired"         // command_failed only: the deadline passed before sending
)

// CommandStatusValue is command_status.status.
type CommandStatusValue string

const (
	StatusPending    CommandStatusValue = "pending"
	StatusSending    CommandStatusValue = "sending"
	StatusSent       CommandStatusValue = "sent"
	StatusAccepted   CommandStatusValue = "accepted"
	StatusConfirmed  CommandStatusValue = "confirmed"
	StatusFailed     CommandStatusValue = "failed"
	StatusExpired    CommandStatusValue = "expired"
	StatusSuperseded CommandStatusValue = "superseded"
)

var CommandStatusValues = []CommandStatusValue{StatusPending, StatusSending, StatusSent, StatusAccepted,
	StatusConfirmed, StatusFailed, StatusExpired, StatusSuperseded}

// Via is how a command was delivered.
type Via string

const (
	ViaLocal Via = "local"
	ViaCloud Via = "cloud"
)

// CommandStatus is the retained JSON on <base>/<id>/command_status. Absent
// values marshal as null so consumers can rely on every key.
type CommandStatus struct {
	Command    Command            `json:"command"`
	ID         *string            `json:"id"`
	Status     CommandStatusValue `json:"status"`
	Via        *Via               `json:"via"`
	Attempts   int                `json:"attempts"`
	Error      *string            `json:"error"`
	ReceivedAt time.Time          `json:"received_at"`
	UpdatedAt  time.Time          `json:"updated_at"`
}

// MarshalJSON writes times in UTC with millisecond precision.
func (s CommandStatus) MarshalJSON() ([]byte, error) {
	type plain CommandStatus
	p := plain(s)
	p.ReceivedAt = p.ReceivedAt.UTC().Truncate(time.Millisecond)
	p.UpdatedAt = p.UpdatedAt.UTC().Truncate(time.Millisecond)
	return json.Marshal(p)
}

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
	TokenExpiresAt    *time.Time      `json:"token_expires_at,omitempty"`
}

// Event is the non-retained JSON on <base>/<id>/event.
type Event struct {
	EventType  EventType `json:"event_type"`
	Reason     string    `json:"reason"`
	Source     *string   `json:"source"`
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

// MaxCommandIDLen bounds the client id echoed in command_status.
const MaxCommandIDLen = 64

var errCommandPayload = errors.New(`use LOCK, UNLOCK or OPEN, or JSON {"command":"LOCK","id":"..."}`)

// ParseCommandMessage accepts a plain LOCK/UNLOCK/OPEN payload or JSON
// {"command": ..., "id": ...}. An invalid id rejects the whole command so it
// never runs under a mangled id.
func ParseCommandMessage(payload []byte) (Command, string, error) {
	text := strings.TrimSpace(string(payload))
	if !strings.HasPrefix(text, "{") {
		if c, ok := ParseCommand(text); ok {
			return c, "", nil
		}
		return "", "", errCommandPayload
	}
	var msg struct {
		Command *string `json:"command"`
		ID      *string `json:"id"`
	}
	if err := json.Unmarshal([]byte(text), &msg); err != nil || msg.Command == nil {
		return "", "", errCommandPayload
	}
	c, ok := ParseCommand(*msg.Command)
	if !ok {
		return "", "", errCommandPayload
	}
	if msg.ID == nil {
		return c, "", nil
	}
	if !validClientID(*msg.ID) {
		return "", "", fmt.Errorf("command id must be at most %d printable characters; %w", MaxCommandIDLen, errCommandPayload)
	}
	return c, *msg.ID, nil
}

// validClientID: a client id echoed back to MQTT is at most MaxCommandIDLen
// printable characters.
func validClientID(id string) bool {
	return len(id) <= MaxCommandIDLen && strings.IndexFunc(id, func(r rune) bool { return !unicode.IsPrint(r) }) < 0
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

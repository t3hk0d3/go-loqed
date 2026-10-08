package bridge

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
)

// MaxClockSkew is ParseEvent's accepted difference between webhook
// TIMESTAMP and now. It covers clock skew and late delivery: the bridge
// delivers to its registered webhooks one after another, so with many of
// them a webhook arrives well after it was signed.
const MaxClockSkew = 20 * time.Second

// Event is a verified webhook from the bridge.
type Event interface{ isEvent() }

// StateReachedEvent: the bolt reached a state (or MOTOR_STALL).
// BoltState is derived from EventType (see loqed.ReachedState);
// RequestedState is only what was asked for.
type StateReachedEvent struct {
	MacWifi, MacBLE string
	EventType       string
	BoltState       loqed.BoltState
	Jammed          bool
	RequestedState  loqed.BoltState
	KeyLocalID      *int
}

// GoToStateEvent: the bolt started moving towards a state
// (event_type GO_TO_STATE_*).
type GoToStateEvent struct {
	MacWifi, MacBLE string
	EventType       string
	GoToState       loqed.BoltState
	KeyLocalID      *int
}

// BatteryEvent: battery report. BatteryPercentage -1 means the lock is offline.
type BatteryEvent struct {
	MacWifi, MacBLE   string
	BatteryType       string
	BatteryPercentage int
	WifiStrength      *int
	BLEStrength       *int
}

// OnlineEvent: signal report. BLEStrength -1 means the lock is offline.
type OnlineEvent struct {
	MacWifi, MacBLE string
	WifiStrength    *int
	BLEStrength     *int
}

func (StateReachedEvent) isEvent() {}
func (GoToStateEvent) isEvent()    {}
func (BatteryEvent) isEvent()      {}
func (OnlineEvent) isEvent()       {}

type rawEvent struct {
	MacWifi           string        `json:"mac_wifi"`
	MacBLE            string        `json:"mac_ble"`
	EventType         *string       `json:"event_type"`
	RequestedState    *loqed.String `json:"requested_state"`
	GoToState         *loqed.String `json:"go_to_state"`
	KeyLocalID        *loqed.KeyID  `json:"key_local_id"`
	BatteryType       *loqed.String `json:"battery_type"`
	BatteryPercentage *loqed.Int    `json:"battery_percentage"`
	WifiStrength      *loqed.Int    `json:"wifi_strength"`
	BLEStrength       *loqed.Int    `json:"ble_strength"`
}

// ParseEvent verifies and decodes a webhook POSTed by the bridge, accepting
// a TIMESTAMP within MaxClockSkew of now.
func ParseEvent(bridgeKey, body []byte, hash, timestamp string, now time.Time) (Event, error) {
	return ParseEventWithin(bridgeKey, body, hash, timestamp, now, MaxClockSkew)
}

// ParseEventWithin is ParseEvent with a TIMESTAMP tolerance of maxSkew; 0
// accepts any age (a captured webhook can then be replayed). hash and
// timestamp are the HASH and TIMESTAMP header values. The hash is checked
// before the timestamp, so only authentic requests can report skew. Skew is
// compared in whole seconds, like loqedAPI.
func ParseEventWithin(bridgeKey, body []byte, hash, timestamp string, now time.Time, maxSkew time.Duration) (Event, error) {
	hash = strings.ToLower(strings.TrimSpace(hash))
	ts, err := strconv.ParseInt(strings.TrimSpace(timestamp), 10, 64)
	if err != nil || hash == "" || ts < 0 {
		return nil, fmt.Errorf("%w: missing or malformed TIMESTAMP/HASH", loqed.ErrBadSignature)
	}
	want := hashHex(body, be64(uint64(ts)), bridgeKey)
	if subtle.ConstantTimeCompare([]byte(want), []byte(hash)) != 1 {
		return nil, fmt.Errorf("%w: HASH mismatch", loqed.ErrBadSignature)
	}
	if limit := int64(maxSkew / time.Second); maxSkew > 0 {
		if skew := now.Unix() - ts; skew > limit || skew < -limit {
			return nil, fmt.Errorf("%w: TIMESTAMP is %ds off, more than %ds (delivered late or clocks differ)", loqed.ErrStaleTimestamp, skew, limit)
		}
	}
	var r rawEvent
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, fmt.Errorf("%w: %w", loqed.ErrInvalidPayload, err)
	}
	return classify(r)
}

func classify(r rawEvent) (Event, error) {
	switch {
	case r.EventType != nil && loqed.IsGoToState(*r.EventType):
		goTo := ""
		if r.GoToState != nil {
			goTo = string(*r.GoToState)
		}
		return GoToStateEvent{MacWifi: r.MacWifi, MacBLE: r.MacBLE, EventType: *r.EventType,
			GoToState: loqed.GoToTarget(*r.EventType, goTo), KeyLocalID: r.KeyLocalID.Ptr()}, nil
	case r.EventType != nil:
		requested := ""
		if r.RequestedState != nil {
			requested = string(*r.RequestedState)
		}
		state, jammed := loqed.ReachedState(*r.EventType)
		return StateReachedEvent{MacWifi: r.MacWifi, MacBLE: r.MacBLE, EventType: *r.EventType,
			BoltState: state, Jammed: jammed, RequestedState: loqed.ParseBoltState(requested),
			KeyLocalID: r.KeyLocalID.Ptr()}, nil
	case r.BatteryPercentage != nil:
		ev := BatteryEvent{MacWifi: r.MacWifi, MacBLE: r.MacBLE, BatteryPercentage: int(*r.BatteryPercentage),
			WifiStrength: intPtr(r.WifiStrength), BLEStrength: intPtr(r.BLEStrength)}
		if r.BatteryType != nil {
			ev.BatteryType = string(*r.BatteryType)
		}
		return ev, nil
	case r.WifiStrength != nil || r.BLEStrength != nil:
		return OnlineEvent{MacWifi: r.MacWifi, MacBLE: r.MacBLE,
			WifiStrength: intPtr(r.WifiStrength), BLEStrength: intPtr(r.BLEStrength)}, nil
	default:
		return nil, fmt.Errorf("%w: unrecognized webhook body", loqed.ErrInvalidPayload)
	}
}

func intPtr(v *loqed.Int) *int {
	if v == nil {
		return nil
	}
	i := int(*v)
	return &i
}

package cloud

import (
	"encoding/json"
	"fmt"

	loqed "github.com/t3hk0d3/go-loqed"
)

// WebhookKind classifies outgoing cloud webhooks.
type WebhookKind int

const (
	KindStateReached WebhookKind = iota + 1
	KindGoToState
	KindSignal
	KindOnline
)

// WebhookEvent is a decoded cloud webhook. Account e-mail, account name and
// admin key name are intentionally not decoded.
type WebhookEvent struct {
	Kind              WebhookKind
	LockID            string
	EventType         string
	BoltState         loqed.BoltState // reached state, derived from EventType (KindStateReached)
	Jammed            bool            // MOTOR_STALL
	RequestedState    loqed.BoltState // requested_state as sent (parsed); informational only
	GoToState         loqed.BoltState // movement target (KindGoToState)
	KeyLocalID        *int
	KeyNameUser       string
	BatteryPercentage *int
	WifiStrength      *int
	BLEStrength       *int
	Online            *bool
}

type rawWebhook struct {
	LockID            loqed.String  `json:"lock_id"`
	EventType         *string       `json:"event_type"`
	RequestedState    *loqed.String `json:"requested_state"`
	GoToState         *loqed.String `json:"go_to_state"`
	KeyLocalID        *loqed.Int    `json:"key_local_id"`
	KeyNameUser       loqed.String  `json:"key_name_user"`
	BatteryPercentage *loqed.Int    `json:"battery_percentage"`
	WifiStrength      *loqed.Int    `json:"wifi_strength"`
	BLEStrength       *loqed.Int    `json:"ble_strength"`
	Online            *loqed.Bool   `json:"online"`
}

// ParseWebhook decodes an outgoing cloud webhook body. Cloud webhooks are
// unsigned; the caller must authenticate the request (path secret).
func ParseWebhook(body []byte) (WebhookEvent, error) {
	var r rawWebhook
	if err := json.Unmarshal(body, &r); err != nil {
		return WebhookEvent{}, fmt.Errorf("%w: %w", loqed.ErrInvalidPayload, err)
	}
	if r.LockID == "" {
		return WebhookEvent{}, fmt.Errorf("%w: cloud webhook without lock_id", loqed.ErrInvalidPayload)
	}
	ev := WebhookEvent{LockID: string(r.LockID), KeyNameUser: string(r.KeyNameUser),
		BatteryPercentage: intPtr(r.BatteryPercentage), WifiStrength: intPtr(r.WifiStrength), BLEStrength: intPtr(r.BLEStrength)}
	if r.KeyLocalID != nil && *r.KeyLocalID >= 0 && *r.KeyLocalID <= 255 {
		ev.KeyLocalID = intPtr(r.KeyLocalID)
	}
	switch {
	case r.EventType != nil && loqed.IsGoToState(*r.EventType):
		goTo := ""
		if r.GoToState != nil {
			goTo = string(*r.GoToState)
		}
		ev.Kind, ev.EventType, ev.GoToState = KindGoToState, *r.EventType, loqed.GoToTarget(*r.EventType, goTo)
	case r.EventType != nil:
		ev.Kind, ev.EventType = KindStateReached, *r.EventType
		ev.BoltState, ev.Jammed = loqed.ReachedState(*r.EventType)
		ev.RequestedState = loqed.BoltUnknown
		if r.RequestedState != nil {
			ev.RequestedState = loqed.ParseBoltState(string(*r.RequestedState))
		}
	case r.Online != nil:
		v := bool(*r.Online)
		ev.Kind, ev.Online = KindOnline, &v
	case r.BatteryPercentage != nil || r.WifiStrength != nil || r.BLEStrength != nil:
		ev.Kind = KindSignal
	default:
		return WebhookEvent{}, fmt.Errorf("%w: unrecognized cloud webhook", loqed.ErrInvalidPayload)
	}
	return ev, nil
}

func intPtr(v *loqed.Int) *int {
	if v == nil {
		return nil
	}
	i := int(*v)
	return &i
}

package bridge

import loqed "github.com/t3hk0d3/go-loqed"

// Status is the bridge's GET /status document.
type Status struct {
	BatteryPercentage  loqed.Int       `json:"battery_percentage"`
	BatteryType        string          `json:"battery_type"`
	BatteryTypeNumeric loqed.Int       `json:"battery_type_numeric"`
	BatteryVoltage     loqed.Float     `json:"battery_voltage"`
	BoltState          loqed.BoltState `json:"bolt_state"`
	BoltStateNumeric   loqed.Int       `json:"bolt_state_numeric"`
	BridgeMacWifi      string          `json:"bridge_mac_wifi"`
	BridgeMacBLE       string          `json:"bridge_mac_ble"`
	LockOnline         loqed.Int       `json:"lock_online"`
	WebhooksNumber     loqed.Int       `json:"webhooks_number"`
	IPAddress          string          `json:"ip_address"`
	UpTimestamp        loqed.Int       `json:"up_timestamp"`
	WifiStrength       loqed.Int       `json:"wifi_strength"`
	BLEStrength        loqed.Int       `json:"ble_strength"`
}

// Action is a lock command understood by /to_lock.
type Action uint8

const (
	ActionOpen   Action = 1 // pull the latch
	ActionUnlock Action = 2 // day_lock
	ActionLock   Action = 3 // night_lock
)

// Triggers selects which events a bridge webhook receives.
// Bit order matches the bridge's flag bitmap.
type Triggers uint32

const (
	TriggerStateChangedOpen Triggers = 1 << iota
	TriggerStateChangedLatch
	TriggerStateChangedNightLock
	TriggerStateChangedUnknown
	TriggerGotoOpen
	TriggerGotoLatch
	TriggerGotoNightLock
	TriggerBattery
	TriggerOnlineStatus
)

// AllTriggers subscribes to every event (511).
const AllTriggers Triggers = 1<<9 - 1

func (t Triggers) bit(f Triggers) int {
	if t&f != 0 {
		return 1
	}
	return 0
}

// Webhook is a registration returned by GET /webhooks.
type Webhook struct {
	ID       loqed.Int
	URL      string
	Triggers Triggers // decoded from the trigger_* flags
}

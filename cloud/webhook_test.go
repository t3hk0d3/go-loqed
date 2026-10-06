package cloud_test

import (
	"errors"
	"fmt"
	"testing"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/cloud"
)

// Payload shapes as received from the LOQED cloud (2026-10-05/06, personal
// values replaced): lock_id is the numeric internal lock id as a JSON number,
// key_local_id is a string ("" without a key), and battery and signal arrive
// as separate events.
const (
	cloudStateReached = `{"event_type":"STATE_CHANGED_NIGHT_LOCK","requested_state":"NIGHT_LOCK","lock_id":6148,"key_local_id":"1",
		"key_name_user":"Gateway","key_name_admin":"Jane Doe","key_account_email":"jane@example.com","key_account_name":"Jane Doe",
		"value1":"STATE_CHANGED_NIGHT_LOCK","value2":"Jane's phone","value3":"jane@example.com"}`
	cloudGoTo = `{"event_type":"GO_TO_STATE_INSTANTOPEN_OPEN","go_to_state":"OPEN","lock_id":6148,"key_local_id":"1",
		"key_name_user":"Gateway","key_name_admin":"Jane Doe","key_account_email":"jane@example.com","key_account_name":"Jane Doe"}`
	cloudNoKey = `{"event_type":"STATE_CHANGED_LATCH","requested_state":"DAY_LOCK","lock_id":6148,"key_local_id":"","key_name_user":"",
		"key_name_admin":"Jane Doe","key_account_email":"jane@example.com","key_account_name":"Jane Doe"}`
	cloudSignal  = `{"ble_strength":88,"lock_id":6148,"wifi_strength":39}`
	cloudBattery = `{"battery_percentage":100,"lock_id":6148}`
	cloudOnline  = `{"lock_id":6148,"online":0}`

	// As shown in the LOQED web API article (alphanumeric lock id,
	// key_account_e-mail, numeric key_local_id, combined signal event).
	cloudDocumented = `{"requested_state":"DAY_LOCK","event_type":"STATE_CHANGED_LATCH","lock_id":"Yq1g","key_local_id":3,
		"key_name_user":"Front door","key_name_admin":"Jane Doe","key_account_e-mail":"jane@example.com","key_account_name":"Jane Doe"}`
	cloudDocumentedSignal = `{"ble_strength":42,"wifi_strength":73,"battery_percentage":88,"lock_id":"Yq1g"}`
)

func TestParseWebhookStateReached(t *testing.T) {
	ev, err := cloud.ParseWebhook([]byte(cloudStateReached))
	if err != nil {
		t.Fatal(err)
	}
	if ev.Kind != cloud.KindStateReached || ev.LockID != "6148" || ev.EventType != "STATE_CHANGED_NIGHT_LOCK" ||
		ev.BoltState != loqed.BoltNightLock || ev.KeyLocalID == nil || *ev.KeyLocalID != 1 || ev.KeyNameUser != "Gateway" {
		t.Fatalf("%+v", ev)
	}
}

func TestParseWebhookDocumentedPayload(t *testing.T) {
	ev, err := cloud.ParseWebhook([]byte(cloudDocumented))
	if err != nil {
		t.Fatal(err)
	}
	if ev.Kind != cloud.KindStateReached || ev.LockID != "Yq1g" || ev.BoltState != loqed.BoltDayLock ||
		ev.KeyLocalID == nil || *ev.KeyLocalID != 3 || ev.KeyNameUser != "Front door" {
		t.Fatalf("%+v", ev)
	}
	s, err := cloud.ParseWebhook([]byte(cloudDocumentedSignal))
	if err != nil || s.Kind != cloud.KindSignal || *s.BLEStrength != 42 || *s.WifiStrength != 73 || *s.BatteryPercentage != 88 {
		t.Fatalf("signal: %+v %v", s, err)
	}
}

func TestParseWebhookNeverExposesPersonalData(t *testing.T) {
	for _, body := range []string{cloudStateReached, cloudGoTo, cloudNoKey, cloudDocumented} {
		ev, err := cloud.ParseWebhook([]byte(body))
		if err != nil {
			t.Fatal(err)
		}
		dump := fmt.Sprintf("%+v", ev)
		for _, leak := range []string{"jane@example.com", "Jane Doe", "Jane's phone"} {
			if contains(dump, leak) {
				t.Fatalf("event exposes %q: %s", leak, dump)
			}
		}
	}
}

func TestParseWebhookEmptyKeyIsNoKey(t *testing.T) {
	ev, err := cloud.ParseWebhook([]byte(cloudNoKey))
	if err != nil {
		t.Fatal(err)
	}
	if ev.KeyLocalID != nil {
		t.Fatalf("expected no key, got %d", *ev.KeyLocalID)
	}
}

func TestParseWebhookFamilies(t *testing.T) {
	g, err := cloud.ParseWebhook([]byte(cloudGoTo))
	if err != nil || g.Kind != cloud.KindGoToState || g.GoToState != loqed.BoltOpen || g.LockID != "6148" || *g.KeyLocalID != 1 {
		t.Fatalf("goto: %+v %v", g, err)
	}
	s, err := cloud.ParseWebhook([]byte(cloudSignal))
	if err != nil || s.Kind != cloud.KindSignal || *s.BLEStrength != 88 || *s.WifiStrength != 39 || s.BatteryPercentage != nil {
		t.Fatalf("signal: %+v %v", s, err)
	}
	b, err := cloud.ParseWebhook([]byte(cloudBattery))
	if err != nil || b.Kind != cloud.KindSignal || *b.BatteryPercentage != 100 || b.BLEStrength != nil {
		t.Fatalf("battery: %+v %v", b, err)
	}
	o, err := cloud.ParseWebhook([]byte(cloudOnline))
	if err != nil || o.Kind != cloud.KindOnline || o.Online == nil || *o.Online || o.LockID != "6148" {
		t.Fatalf("online: %+v %v", o, err)
	}
}

func TestParseWebhookStateFromEventType(t *testing.T) {
	ev, err := cloud.ParseWebhook([]byte(`{"requested_state":"NIGHT_LOCK","event_type":"MOTOR_STALL","lock_id":"x","key_local_id":999}`))
	if err != nil {
		t.Fatal(err)
	}
	if !ev.Jammed || ev.BoltState != loqed.BoltUnknown || ev.RequestedState != loqed.BoltNightLock {
		t.Fatalf("%+v", ev)
	}
	if ev.KeyLocalID != nil {
		t.Fatalf("out-of-range key id kept: %d", *ev.KeyLocalID)
	}
}

func TestParseWebhookNumericKeyNameUser(t *testing.T) {
	ev, err := cloud.ParseWebhook([]byte(`{"event_type":"STATE_CHANGED_LATCH","lock_id":"x","key_name_user":1234}`))
	if err != nil || ev.KeyNameUser != "1234" {
		t.Fatalf("%+v %v", ev, err)
	}
}

func TestParseWebhookInvalid(t *testing.T) {
	for _, body := range []string{`nope`, `{"online":1}`, `{"lock_id":"x"}`} {
		if _, err := cloud.ParseWebhook([]byte(body)); !errors.Is(err, loqed.ErrInvalidPayload) {
			t.Errorf("%s: got %v", body, err)
		}
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

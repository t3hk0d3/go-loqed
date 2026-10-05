package cloud_test

import (
	"errors"
	"fmt"
	"testing"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/cloud"
)

// Payloads from the LOQED web API documentation (June 2026).
const (
	cloudStateReached = `{"requested_state":"DAY_LOCK","event_type":"STATE_CHANGED_LATCH","lock_id":"Yq1g","key_local_id":3,
		"key_name_user":"Front door","key_name_admin":"Jane Doe","key_account_e-mail":"jane@example.com","key_account_name":"Jane Doe"}`
	cloudGoTo   = `{"go_to_state":"OPEN","event_type":"GO_TO_STATE_INSTANTOPEN_OPEN","lock_id":"Yq1g","key_local_id":"3","key_name_user":"Front door"}`
	cloudSignal = `{"ble_strength":42,"wifi_strength":73,"battery_percentage":88,"lock_id":"Yq1g"}`
	cloudOnline = `{"online":1,"lock_id":"Yq1g"}`
	// As observed on 2026-10-05 (personal values replaced).
	cloudObserved = `{"event_type":"STATE_CHANGED_NIGHT_LOCK","requested_state":"NIGHT_LOCK","lock_id":6148,"key_local_id":"1",
		"key_name_user":"Gateway","key_name_admin":"Jane Doe","key_account_email":"jane@example.com","key_account_name":"Jane Doe",
		"value1":"STATE_CHANGED_NIGHT_LOCK","value2":"Jane's phone","value3":"jane@example.com"}`
	cloudNoKey = `{"event_type":"STATE_CHANGED_LATCH","requested_state":"DAY_LOCK","lock_id":"6148","key_local_id":"","key_name_user":""}`
)

func TestParseWebhookStateReached(t *testing.T) {
	ev, err := cloud.ParseWebhook([]byte(cloudStateReached))
	if err != nil {
		t.Fatal(err)
	}
	if ev.Kind != cloud.KindStateReached || ev.LockID != "Yq1g" || ev.EventType != "STATE_CHANGED_LATCH" ||
		ev.BoltState != loqed.BoltDayLock || *ev.KeyLocalID != 3 || ev.KeyNameUser != "Front door" {
		t.Fatalf("%+v", ev)
	}
}

func TestParseWebhookNeverExposesPersonalData(t *testing.T) {
	ev, err := cloud.ParseWebhook([]byte(cloudStateReached))
	if err != nil {
		t.Fatal(err)
	}
	dump := fmt.Sprintf("%+v", ev)
	for _, leak := range []string{"jane@example.com", "Jane Doe"} {
		if contains(dump, leak) {
			t.Fatalf("event exposes %q: %s", leak, dump)
		}
	}
}

func TestParseWebhookObservedPayload(t *testing.T) {
	ev, err := cloud.ParseWebhook([]byte(cloudObserved))
	if err != nil {
		t.Fatal(err)
	}
	if ev.LockID != "6148" || ev.KeyLocalID == nil || *ev.KeyLocalID != 1 || ev.BoltState != loqed.BoltNightLock {
		t.Fatalf("%+v", ev)
	}
	dump := fmt.Sprintf("%+v", ev)
	for _, leak := range []string{"jane@example.com", "Jane Doe", "Jane's phone"} {
		if contains(dump, leak) {
			t.Fatalf("event exposes %q: %s", leak, dump)
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
	if err != nil || g.Kind != cloud.KindGoToState || g.GoToState != loqed.BoltOpen || *g.KeyLocalID != 3 {
		t.Fatalf("goto: %+v %v", g, err)
	}
	s, err := cloud.ParseWebhook([]byte(cloudSignal))
	if err != nil || s.Kind != cloud.KindSignal || *s.BLEStrength != 42 || *s.WifiStrength != 73 || *s.BatteryPercentage != 88 {
		t.Fatalf("signal: %+v %v", s, err)
	}
	o, err := cloud.ParseWebhook([]byte(cloudOnline))
	if err != nil || o.Kind != cloud.KindOnline || o.Online == nil || !*o.Online {
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

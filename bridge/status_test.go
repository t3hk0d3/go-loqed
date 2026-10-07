package bridge_test

import (
	"context"
	"errors"
	"net/http"
	"testing"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/bridge"
)

// From HA core tests/components/loqed/fixtures/status_ok.json.
const statusOK = `{
  "battery_percentage": 78, "battery_type": "NICKEL_METAL_HYDRIDE", "battery_type_numeric": 1,
  "battery_voltage": 10.37, "bolt_state": "day_lock", "bolt_state_numeric": 2,
  "bridge_mac_wifi": "aa:bb:cc:dd:ee:ff", "bridge_mac_ble": "11:22:33:44:55:66",
  "lock_online": 1, "webhooks_number": 1, "ip_address": "192.168.42.12",
  "up_timestamp": 1653041994, "wifi_strength": 73, "ble_strength": 20
}`

func TestStatus(t *testing.T) {
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/status" {
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "text/html") // the bridge really does this
		_, _ = w.Write([]byte(statusOK))
	}))
	st, err := c.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if st.BatteryPercentage != 78 || st.BatteryVoltage != 10.37 || st.BoltState != loqed.BoltDayLock ||
		st.LockOnline != 1 || st.WifiStrength != 73 || st.BLEStrength != 20 || st.BridgeMacWifi != "aa:bb:cc:dd:ee:ff" {
		t.Fatalf("unexpected status: %+v", st)
	}
}

func TestStatusAcceptsStringTypedNumbers(t *testing.T) {
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"battery_percentage":"78","bolt_state":"NIGHT_LOCK","lock_online":"1","ble_strength":"-1","battery_voltage":"10.1"}`))
	}))
	st, err := c.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if st.BatteryPercentage != 78 || st.BoltState != loqed.BoltNightLock || st.LockOnline != 1 || st.BLEStrength != -1 {
		t.Fatalf("unexpected status: %+v", st)
	}
}

func TestStatusMissingBoltStateIsUnknown(t *testing.T) {
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"battery_percentage":78}`))
	}))
	st, err := c.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if st.BoltState != loqed.BoltUnknown {
		t.Fatalf("bolt state %q", st.BoltState)
	}
}

func TestNewRejectsHostnames(t *testing.T) {
	for _, h := range []string{"loqed-bridge.local", "bad host:99999", "", "http://192.0.2.1"} {
		if _, err := bridge.New(h, bridge.Credentials{}); err == nil {
			t.Errorf("%q: expected error", h)
		}
	}
	for _, h := range []string{"192.0.2.1", "192.0.2.1:8080", "[2001:db8::1]:80", "2001:db8::1"} {
		if _, err := bridge.New(h, bridge.Credentials{}); err != nil {
			t.Errorf("%q: %v", h, err)
		}
	}
}

func TestStatusInvalidJSON(t *testing.T) {
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`<html>`))
	}))
	_, err := c.Status(context.Background())
	if !errors.Is(err, loqed.ErrInvalidPayload) {
		t.Fatalf("got %v", err)
	}
}

func TestNewRejectsBadBase64(t *testing.T) {
	if _, err := bridge.New("192.0.2.1", bridge.Credentials{BridgeKey: "%%%"}); err == nil {
		t.Fatal("expected error for bad bridge key")
	}
	if _, err := bridge.New("192.0.2.1", bridge.Credentials{KeySecret: "%%%"}); err == nil {
		t.Fatal("expected error for bad key secret")
	}
}

func TestBridgeKeyReturnsCopy(t *testing.T) {
	c, err := bridge.New("192.0.2.1", bridge.Credentials{BridgeKey: testBridgeKey})
	if err != nil {
		t.Fatal(err)
	}
	k := c.BridgeKey()
	k[0] ^= 0xff
	if string(c.BridgeKey()) != "bonjour monde" {
		t.Fatal("BridgeKey must return a copy")
	}
}

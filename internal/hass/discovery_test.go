package hass_test

import (
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/t3hk0d3/go-loqed/internal/hass"
)

var update = flag.Bool("update", false, "rewrite golden files")

var topics = hass.Topics{Base: "loqed", DiscoveryPrefix: "homeassistant"}

func TestTopics(t *testing.T) {
	cases := map[string]string{
		topics.Status():            "loqed/status",
		topics.Availability("abc"): "loqed/abc/availability",
		topics.State("abc"):        "loqed/abc/state",
		topics.Event("abc"):        "loqed/abc/event",
		topics.Command("abc"):      "loqed/abc/command",
		topics.CommandWildcard():   "loqed/+/command",
		topics.Discovery("abc"):    "homeassistant/device/loqed_abc/config",
		topics.HAStatus():          "homeassistant/status",
	}
	for got, want := range cases {
		if got != want {
			t.Errorf("got %s want %s", got, want)
		}
	}
	if hass.TopicID("Yq1g/K4+#x y") != "Yq1g_K4__x_y" {
		t.Errorf("TopicID: %s", hass.TopicID("Yq1g/K4+#x y"))
	}
}

func TestDiscoveryPayload(t *testing.T) {
	b, err := hass.DiscoveryPayload(topics, hass.LockInfo{ID: "lock1", Name: "Front door", Model: "LOQED Touch", MacWifi: "AA:BB:CC:DD:EE:FF"}, "1.2.3")
	if err != nil {
		t.Fatal(err)
	}
	var p struct {
		Device struct {
			Identifiers  []string   `json:"identifiers"`
			Name         string     `json:"name"`
			Manufacturer string     `json:"manufacturer"`
			Connections  [][]string `json:"connections"`
		} `json:"device"`
		Availability     []map[string]string       `json:"availability"`
		AvailabilityMode string                    `json:"availability_mode"`
		Components       map[string]map[string]any `json:"components"`
	}
	if err := json.Unmarshal(b, &p); err != nil {
		t.Fatal(err)
	}
	if p.Device.Identifiers[0] != "loqed_lock1" || p.Device.Name != "Front door" || p.Device.Manufacturer != "LOQED" ||
		p.Device.Connections[0][1] != "aa:bb:cc:dd:ee:ff" {
		t.Fatalf("device %+v", p.Device)
	}
	if p.AvailabilityMode != "all" || p.Availability[0]["topic"] != "loqed/status" || p.Availability[1]["topic"] != "loqed/lock1/availability" {
		t.Fatalf("availability %+v %s", p.Availability, p.AvailabilityMode)
	}
	wantPlatforms := map[string]string{"lock": "lock", "battery": "sensor", "battery_voltage": "sensor", "wifi_signal": "sensor",
		"ble_signal": "sensor", "lock_online": "binary_sensor", "state_stale": "binary_sensor", "connection_mode": "sensor", "last_change_reason": "sensor", "event": "event"}
	if len(p.Components) != len(wantPlatforms) {
		t.Fatalf("components %v", p.Components)
	}
	for id, platform := range wantPlatforms {
		c := p.Components[id]
		if c["platform"] != platform || c["unique_id"] != "loqed_lock1_"+id {
			t.Errorf("%s: %v", id, c)
		}
	}
	if p.Components["lock"]["command_topic"] != "loqed/lock1/command" || p.Components["event"]["state_topic"] != "loqed/lock1/event" {
		t.Errorf("topics wrong")
	}
	if p.Components["lock"]["json_attributes_topic"] != "loqed/lock1/state" || p.Components["lock"]["json_attributes_template"] == nil {
		t.Errorf("lock must expose state_stale as attributes: %v", p.Components["lock"])
	}
	if p.Components["state_stale"]["entity_category"] != "diagnostic" || p.Components["state_stale"]["device_class"] != "problem" {
		t.Errorf("state_stale sensor: %v", p.Components["state_stale"])
	}
	if p.Components["lock"]["retain"] != nil {
		t.Errorf("commands must not be retained")
	}

	golden := filepath.Join("testdata", "discovery_lock1.golden.json")
	var pretty bytes.Buffer
	_ = json.Indent(&pretty, b, "", "  ")
	if *update {
		_ = os.MkdirAll("testdata", 0o755)
		if err := os.WriteFile(golden, pretty.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatalf("missing golden file; run go test ./internal/hass -run TestDiscoveryPayload -update: %v", err)
	}
	if !bytes.Equal(want, pretty.Bytes()) {
		t.Fatalf("discovery payload changed; review and run with -update.\n got: %s", pretty.Bytes())
	}
}

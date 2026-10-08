package hass_test

import (
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/t3hk0d3/go-loqed/internal/model"
	"github.com/t3hk0d3/go-loqed/internal/mqtt"
	"github.com/t3hk0d3/go-loqed/internal/mqtt/hass"
)

var update = flag.Bool("update", false, "rewrite golden files")

var discovery = hass.New("homeassistant", mqtt.Topics{Base: "loqed"}, "1.2.3")

// The interface is the only way the MQTT client learns about Home Assistant.
var _ mqtt.Discovery = discovery

func TestDiscoveryTopic(t *testing.T) {
	if got := discovery.Topic("Yq1g/K4"); got != "homeassistant/device/loqed_Yq1g_K4/config" {
		t.Errorf("got %s", got)
	}
}

func TestBirthTopic(t *testing.T) {
	if got := discovery.BirthTopic(); got != "homeassistant/status" {
		t.Errorf("got %s", got)
	}
}

func TestDiscoveryPayload(t *testing.T) {
	b, err := discovery.Payload(mqtt.LockInfo{ID: "lock1", Name: "Front door", Model: "LOQED Touch", MacWifi: "AA:BB:CC:DD:EE:FF"})
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
		"ble_signal": "sensor", "lock_online": "binary_sensor", "state_stale": "binary_sensor", "connection_mode": "sensor", "last_change_reason": "sensor", "event": "event",
		"last_command": "sensor", "token_expires": "sensor", "bridge_webhooks": "sensor"}
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
	last := p.Components["last_command"]
	if last["state_topic"] != "loqed/lock1/command_status" || last["value_template"] != "{{ value_json.status }}" ||
		last["device_class"] != "enum" || last["entity_category"] != "diagnostic" || last["json_attributes_topic"] != "loqed/lock1/command_status" {
		t.Errorf("last command sensor: %v", last)
	}
	if opts, _ := last["options"].([]any); len(opts) != len(model.CommandStatusValues) || opts[0] != "pending" {
		t.Errorf("last command options: %v", last["options"])
	}
	if te := p.Components["token_expires"]; te["device_class"] != "timestamp" || te["entity_category"] != "diagnostic" ||
		te["state_topic"] != "loqed/lock1/state" {
		t.Errorf("token expires sensor: %v", te)
	}
	for _, id := range []string{"wifi_signal", "ble_signal"} {
		if p.Components[id]["unit_of_measurement"] != "%" {
			t.Errorf("%s must be in %%: %v", id, p.Components[id])
		}
	}
	bleAvail, _ := p.Components["ble_signal"]["availability"].([]any)
	if len(bleAvail) != 3 || p.Components["ble_signal"]["availability_mode"] != "all" {
		t.Errorf("BLE sensor must be unavailable at -1: %v", p.Components["ble_signal"])
	}
	hooks := p.Components["bridge_webhooks"]
	if hooks["state_topic"] != "loqed/lock1/webhooks" || hooks["value_template"] != "{{ value_json.count }}" ||
		hooks["entity_category"] != "diagnostic" || hooks["json_attributes_topic"] != "loqed/lock1/webhooks" {
		t.Errorf("bridge webhooks sensor: %v", hooks)
	}
	if tpl, _ := hooks["json_attributes_template"].(string); !strings.Contains(tpl, "value_json.webhooks") ||
		!strings.Contains(tpl, "value_json.revision") || !strings.Contains(tpl, "value_json.fetched_at") {
		t.Errorf("bridge webhooks attributes: %v", hooks["json_attributes_template"])
	}
	for id, c := range p.Components {
		if topic, _ := c["command_topic"].(string); strings.Contains(topic, "webhooks") {
			t.Errorf("%s writes webhooks; no entity may", id)
		}
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
		t.Fatalf("missing golden file; run go test ./internal/mqtt/hass -run TestDiscoveryPayload -update: %v", err)
	}
	if !bytes.Equal(want, pretty.Bytes()) {
		t.Fatalf("discovery payload changed; review and run with -update.\n got: %s", pretty.Bytes())
	}
}

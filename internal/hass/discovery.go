package hass

import (
	"encoding/json"
	"strings"

	"github.com/t3hk0d3/go-loqed/internal/model"
)

type LockInfo struct {
	ID      string
	Name    string
	Model   string
	MacWifi string
}

// DiscoveryPayload builds the device-based discovery message: one device
// per lock with all of its entities as components.
func DiscoveryPayload(t Topics, l LockInfo, version string) ([]byte, error) {
	tid := TopicID(l.ID)
	uid := "loqed_" + tid
	state := t.State(tid)
	modelName := l.Model
	if modelName == "" {
		modelName = "Smart lock"
	}
	device := map[string]any{"identifiers": []string{uid}, "name": l.Name, "manufacturer": "LOQED", "model": modelName}
	if l.MacWifi != "" {
		device["connections"] = [][]string{{"mac", strings.ToLower(l.MacWifi)}}
	}
	sensor := func(id, name, field string, extra map[string]any) map[string]any {
		c := map[string]any{"platform": "sensor", "name": name, "unique_id": uid + "_" + id, "state_topic": state,
			"value_template": "{{ value_json." + field + " }}"}
		for k, v := range extra {
			c[k] = v
		}
		return c
	}
	statuses := make([]string, len(model.CommandStatusValues))
	for i, v := range model.CommandStatusValues {
		statuses[i] = string(v)
	}
	cmdStatus := t.CommandStatus(tid)
	eventTypes := make([]string, len(model.EventTypes))
	for i, e := range model.EventTypes {
		eventTypes[i] = string(e)
	}
	components := map[string]any{
		"lock": map[string]any{
			"platform": "lock", "name": nil, "unique_id": uid + "_lock",
			"state_topic": state, "value_template": "{{ value_json.lock }}",
			"command_topic": t.Command(tid), "qos": 1,
			"payload_lock": string(model.CommandLock), "payload_unlock": string(model.CommandUnlock), "payload_open": string(model.CommandOpen),
			"state_locked": string(model.Locked), "state_unlocked": string(model.Unlocked), "state_open": string(model.Open),
			"state_locking": string(model.Locking), "state_unlocking": string(model.Unlocking), "state_opening": string(model.Opening),
			"state_jammed":          string(model.Jammed),
			"json_attributes_topic": state,
			"json_attributes_template": "{{ {'state_stale': value_json.state_stale, 'mode': value_json.mode, " +
				"'last_event_at': value_json.last_event_at} | tojson }}",
		},
		"battery": sensor("battery", "Battery", "battery_percentage",
			map[string]any{"device_class": "battery", "unit_of_measurement": "%", "state_class": "measurement"}),
		"battery_voltage": sensor("battery_voltage", "Battery voltage", "battery_voltage",
			map[string]any{"device_class": "voltage", "unit_of_measurement": "V", "state_class": "measurement", "entity_category": "diagnostic"}),
		"wifi_signal": sensor("wifi_signal", "Wi-Fi signal", "wifi_strength",
			map[string]any{"unit_of_measurement": "%", "state_class": "measurement", "entity_category": "diagnostic"}),
		// ble_strength -1 means the lock itself is offline: no reading then.
		"ble_signal": sensor("ble_signal", "Bluetooth signal", "ble_strength",
			map[string]any{"unit_of_measurement": "%", "state_class": "measurement", "entity_category": "diagnostic",
				"availability": []map[string]string{{"topic": t.Status()}, {"topic": t.Availability(tid)},
					{"topic": state, "value_template": "{{ 'offline' if value_json.ble_strength == -1 else 'online' }}"}},
				"availability_mode": "all"}),
		"lock_online": map[string]any{
			"platform": "binary_sensor", "name": "Lock online", "unique_id": uid + "_lock_online", "state_topic": state,
			"value_template": "{{ 'ON' if value_json.lock_online else 'OFF' }}", "device_class": "connectivity", "entity_category": "diagnostic",
		},
		"state_stale": map[string]any{
			"platform": "binary_sensor", "name": "State stale", "unique_id": uid + "_state_stale", "state_topic": state,
			"value_template": "{{ 'ON' if value_json.state_stale else 'OFF' }}", "device_class": "problem", "entity_category": "diagnostic",
		},
		"connection_mode": sensor("connection_mode", "Connection mode", "mode",
			map[string]any{"device_class": "enum", "options": []string{"local", "cloud", "offline"}, "entity_category": "diagnostic"}),
		"last_change_reason": sensor("last_change_reason", "Last change reason", "last_event", map[string]any{
			"json_attributes_topic": state,
			"json_attributes_template": "{{ {'key_local_id': value_json.last_key_id, 'key_name': value_json.last_key_name, " +
				"'last_event_at': value_json.last_event_at} | tojson }}",
		}),
		"last_command": map[string]any{
			"platform": "sensor", "name": "Last command", "unique_id": uid + "_last_command", "state_topic": cmdStatus,
			"value_template": "{{ value_json.status }}", "device_class": "enum", "options": statuses, "entity_category": "diagnostic",
			"json_attributes_topic": cmdStatus,
			"json_attributes_template": "{{ {'command': value_json.command, 'id': value_json.id, 'via': value_json.via, " +
				"'attempts': value_json.attempts, 'error': value_json.error, 'updated_at': value_json.updated_at} | tojson }}",
		},
		"token_expires": sensor("token_expires", "Token expires", "token_expires_at", map[string]any{
			"device_class": "timestamp", "entity_category": "diagnostic",
			"value_template": "{{ value_json.token_expires_at if value_json.token_expires_at is defined else None }}",
		}),
		"event": map[string]any{
			"platform": "event", "name": "Lock event", "unique_id": uid + "_event", "state_topic": t.Event(tid), "event_types": eventTypes,
		},
	}
	return json.Marshal(map[string]any{
		"device":            device,
		"origin":            map[string]any{"name": "loqed-mqtt", "sw_version": version, "support_url": "https://github.com/t3hk0d3/go-loqed"},
		"components":        components,
		"availability":      []map[string]string{{"topic": t.Status()}, {"topic": t.Availability(tid)}},
		"availability_mode": "all",
		"qos":               1,
	})
}

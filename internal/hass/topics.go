// Package hass publishes lock state to MQTT and Home Assistant discovery.
package hass

import "strings"

type Topics struct {
	Base            string
	DiscoveryPrefix string
}

func (t Topics) Status() string                { return t.Base + "/status" }
func (t Topics) Availability(id string) string { return t.Base + "/" + id + "/availability" }
func (t Topics) State(id string) string        { return t.Base + "/" + id + "/state" }
func (t Topics) Event(id string) string        { return t.Base + "/" + id + "/event" }
func (t Topics) Command(id string) string      { return t.Base + "/" + id + "/command" }
func (t Topics) CommandWildcard() string       { return t.Base + "/+/command" }
func (t Topics) CommandStatus(id string) string {
	return t.Base + "/" + id + "/command_status"
}
func (t Topics) Discovery(id string) string {
	return t.DiscoveryPrefix + "/device/loqed_" + id + "/config"
}
func (t Topics) HAStatus() string { return t.DiscoveryPrefix + "/status" }

// TopicID makes a lock id safe for use as one MQTT topic level.
func TopicID(lockID string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_', r == '-':
			return r
		default:
			return '_'
		}
	}, lockID)
}

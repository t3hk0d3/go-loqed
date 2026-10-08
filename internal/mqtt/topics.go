// Package mqtt is the gateway's MQTT client: it owns the <base>/... topics,
// publishes state, events and command status, and receives commands. It knows
// nothing about Home Assistant; discovery is plugged in through Discovery.
package mqtt

import "strings"

type Topics struct {
	Base string
}

func (t Topics) Status() string                { return t.Base + "/status" }
func (t Topics) Availability(id string) string { return t.Base + "/" + id + "/availability" }
func (t Topics) State(id string) string        { return t.Base + "/" + id + "/state" }
func (t Topics) Event(id string) string        { return t.Base + "/" + id + "/event" }
func (t Topics) Command(id string) string      { return t.Base + "/" + id + "/command" }
func (t Topics) CommandWildcard() string       { return t.Base + "/+/command" }
func (t Topics) CloudWebhook(id string) string {
	return t.Base + "/" + id + "/cloud_webhook"
}
func (t Topics) CloudWebhookWildcard() string { return t.Base + "/+/cloud_webhook" }
func (t Topics) CommandStatus(id string) string {
	return t.Base + "/" + id + "/command_status"
}
func (t Topics) Webhooks(id string) string       { return t.Base + "/" + id + "/webhooks" }
func (t Topics) WebhooksSet(id string) string    { return t.Base + "/" + id + "/webhooks/set" }
func (t Topics) WebhooksSetWildcard() string     { return t.Base + "/+/webhooks/set" }
func (t Topics) WebhooksResult(id string) string { return t.Base + "/" + id + "/webhooks/result" }

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

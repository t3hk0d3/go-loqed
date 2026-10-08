package model

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"
	"time"

	"github.com/t3hk0d3/go-loqed/bridge"
)

// Limits of a SetWebhooks request (spec 5.9).
const (
	MaxWebhooksPayload = 16 << 10
	MaxWebhooksEntries = 20
	MaxWebhookURLLen   = 1024
)

// WebhookList is the retained JSON on <base>/<id>/webhooks.
type WebhookList struct {
	Revision  string         `json:"revision"`
	FetchedAt time.Time      `json:"fetched_at"`
	Count     int            `json:"count"`
	Webhooks  []WebhookEntry `json:"webhooks"`
}

// MarshalJSON writes fetched_at in UTC with second precision.
func (l WebhookList) MarshalJSON() ([]byte, error) {
	type plain WebhookList
	p := plain(l)
	p.FetchedAt = p.FetchedAt.UTC().Truncate(time.Second)
	if p.Webhooks == nil {
		p.Webhooks = []WebhookEntry{}
	}
	return json.Marshal(p)
}

// WebhookEntry is one bridge webhook; Gateway marks the gateway's own.
type WebhookEntry struct {
	ID       int      `json:"id"`
	URL      string   `json:"url"`
	Triggers []string `json:"triggers"`
	Gateway  bool     `json:"gateway"`
}

// WebhooksRequest is a SetWebhooks request: the complete desired list.
type WebhooksRequest struct {
	Revision  string
	RequestID *string
	Webhooks  []WebhookSpec
}

// WebhookSpec is one entry of a request: by ID or by URL. A URL entry
// without triggers asks for all of them.
type WebhookSpec struct {
	ID          *int
	URL         *string
	Triggers    bridge.Triggers
	HasTriggers bool
}

// WebhooksInvalidError is a request rejected before any bridge call. Detail
// names the field or entry, never a URL.
type WebhooksInvalidError struct {
	Detail    string
	RequestID *string
}

func (e *WebhooksInvalidError) Error() string { return "invalid SetWebhooks request: " + e.Detail }

// SetWebhooks result status and the error classes of its own.
const (
	WebhooksOK      = "ok"
	WebhooksPartial = "partial"
	WebhooksFailed  = "failed"

	WebhooksErrInvalid  = "invalid"
	WebhooksErrConflict = "conflict"
	WebhooksErrOffline  = "offline"
)

// WebhooksResult is the JSON on <base>/<id>/webhooks/result. Absent values
// marshal as null and empty id lists as [].
type WebhooksResult struct {
	RequestID *string `json:"request_id"`
	Status    string  `json:"status"`
	Error     *string `json:"error"`
	Detail    *string `json:"detail"`
	Removed   []int   `json:"removed"`
	Added     []int   `json:"added"`
	Kept      []int   `json:"kept"`
	Revision  *string `json:"revision"`
}

func (r WebhooksResult) MarshalJSON() ([]byte, error) {
	type plain WebhooksResult
	p := plain(r)
	for _, l := range []*[]int{&p.Removed, &p.Added, &p.Kept} {
		if *l == nil {
			*l = []int{}
		}
	}
	return json.Marshal(p)
}

// ParseWebhooksRequest decodes a request and checks everything that does not
// depend on the bridge's current list.
func ParseWebhooksRequest(payload []byte) (WebhooksRequest, error) {
	if len(payload) > MaxWebhooksPayload {
		return WebhooksRequest{}, &WebhooksInvalidError{Detail: fmt.Sprintf("payload too large (max %d bytes)", MaxWebhooksPayload)}
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(payload, &top); err != nil || top == nil {
		return WebhooksRequest{}, &WebhooksInvalidError{Detail: "payload is not a JSON object"}
	}
	var req WebhooksRequest
	if raw, ok := top["request_id"]; ok && !isNull(raw) {
		var id string
		if err := json.Unmarshal(raw, &id); err != nil || !validClientID(id) {
			return WebhooksRequest{}, &WebhooksInvalidError{Detail: fmt.Sprintf("request_id must be at most %d printable characters", MaxCommandIDLen)}
		}
		req.RequestID = &id
	}
	invalid := func(format string, args ...any) error {
		return &WebhooksInvalidError{Detail: fmt.Sprintf(format, args...), RequestID: req.RequestID}
	}
	if raw, ok := top["revision"]; !ok || json.Unmarshal(raw, &req.Revision) != nil || req.Revision == "" {
		return WebhooksRequest{}, invalid("revision missing")
	}
	var entries []map[string]json.RawMessage
	if raw, ok := top["webhooks"]; !ok || isNull(raw) || json.Unmarshal(raw, &entries) != nil {
		return WebhooksRequest{}, invalid("webhooks missing or not a list of objects")
	}
	if len(entries) > MaxWebhooksEntries {
		return WebhooksRequest{}, invalid("webhooks may list at most %d entries", MaxWebhooksEntries)
	}
	req.Webhooks = make([]WebhookSpec, 0, len(entries))
	for i, e := range entries {
		spec, why := parseWebhookSpec(e)
		if why != "" {
			return WebhooksRequest{}, invalid("webhooks[%d]: %s", i, why)
		}
		req.Webhooks = append(req.Webhooks, spec)
	}
	return req, nil
}

// parseWebhookSpec returns the entry or why it is invalid.
func parseWebhookSpec(e map[string]json.RawMessage) (WebhookSpec, string) {
	var spec WebhookSpec
	for k := range e {
		if k != "id" && k != "url" && k != "triggers" {
			return spec, fmt.Sprintf("unknown key %q", k)
		}
	}
	rawID, hasID := e["id"]
	rawURL, hasURL := e["url"]
	if hasID == hasURL {
		return spec, "give exactly one of id and url"
	}
	if hasID {
		var id int
		if err := json.Unmarshal(rawID, &id); err != nil {
			return spec, "id must be an integer"
		}
		spec.ID = &id
	} else {
		var u string
		if err := json.Unmarshal(rawURL, &u); err != nil {
			return spec, "url must be a string"
		}
		if why := checkWebhookURL(u); why != "" {
			return spec, why
		}
		spec.URL = &u
		spec.Triggers = bridge.AllTriggers
	}
	if raw, ok := e["triggers"]; ok {
		var names []string
		if err := json.Unmarshal(raw, &names); err != nil {
			return spec, "triggers must be a list of names"
		}
		t, err := bridge.ParseTriggers(names)
		if err != nil {
			return spec, "triggers: " + err.Error()
		}
		spec.Triggers, spec.HasTriggers = t, true
	}
	return spec, ""
}

func checkWebhookURL(u string) string {
	if len(u) > MaxWebhookURLLen {
		return fmt.Sprintf("url longer than %d bytes", MaxWebhookURLLen)
	}
	p, err := url.Parse(u)
	if err != nil || (p.Scheme != "http" && p.Scheme != "https") || p.Host == "" {
		return "url must be an absolute http or https URL with a host"
	}
	return ""
}

func isNull(raw json.RawMessage) bool { return bytes.Equal(bytes.TrimSpace(raw), []byte("null")) }

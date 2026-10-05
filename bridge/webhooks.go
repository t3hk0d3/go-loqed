package bridge

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	loqed "github.com/t3hk0d3/go-loqed"
)

// signedHeaders builds TIMESTAMP and HASH = hex(sha256(prefix... | ts8 | bridgeKey)).
func (c *Client) signedHeaders(prefix ...[]byte) map[string]string {
	ts := c.now().Unix()
	parts := make([][]byte, 0, len(prefix)+2)
	parts = append(parts, prefix...)
	parts = append(parts, be64(uint64(ts)), c.bridgeKey)
	return map[string]string{
		"TIMESTAMP": strconv.FormatInt(ts, 10),
		"HASH":      hashHex(parts...),
	}
}

// ListWebhooks returns all webhooks registered on the bridge.
func (c *Client) ListWebhooks(ctx context.Context) ([]Webhook, error) {
	body, err := c.do(ctx, http.MethodGet, "/webhooks", nil, c.signedHeaders())
	if err != nil {
		return nil, err
	}
	var hooks []Webhook
	if err := json.Unmarshal(body, &hooks); err != nil {
		return nil, fmt.Errorf("%w: webhooks: %w", loqed.ErrInvalidPayload, err)
	}
	return hooks, nil
}

type webhookRequest struct {
	URL                   string `json:"url"`
	StateChangedOpen      int    `json:"trigger_state_changed_open"`
	StateChangedLatch     int    `json:"trigger_state_changed_latch"`
	StateChangedNightLock int    `json:"trigger_state_changed_night_lock"`
	StateChangedUnknown   int    `json:"trigger_state_changed_unknown"`
	GotoOpen              int    `json:"trigger_state_goto_open"`
	GotoLatch             int    `json:"trigger_state_goto_latch"`
	GotoNightLock         int    `json:"trigger_state_goto_night_lock"`
	Battery               int    `json:"trigger_battery"`
	OnlineStatus          int    `json:"trigger_online_status"`
}

// CreateWebhook registers url for the selected triggers.
// HASH = sha256(url | triggers as u32 BE | ts8 | bridgeKey).
func (c *Client) CreateWebhook(ctx context.Context, url string, t Triggers) error {
	t &= AllTriggers // the body carries bits 0..8 only; the hash must agree
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false) // match Python json.dumps: '&' stays literal
	err := enc.Encode(webhookRequest{
		URL:                   url,
		StateChangedOpen:      t.bit(TriggerStateChangedOpen),
		StateChangedLatch:     t.bit(TriggerStateChangedLatch),
		StateChangedNightLock: t.bit(TriggerStateChangedNightLock),
		StateChangedUnknown:   t.bit(TriggerStateChangedUnknown),
		GotoOpen:              t.bit(TriggerGotoOpen),
		GotoLatch:             t.bit(TriggerGotoLatch),
		GotoNightLock:         t.bit(TriggerGotoNightLock),
		Battery:               t.bit(TriggerBattery),
		OnlineStatus:          t.bit(TriggerOnlineStatus),
	})
	if err != nil {
		return err
	}
	body := bytes.TrimSuffix(buf.Bytes(), []byte("\n"))
	_, err = c.do(ctx, http.MethodPost, "/webhooks", body, c.signedHeaders([]byte(url), be32(uint32(t))))
	return err
}

// DeleteWebhook removes a registration. HASH = sha256(id as u64 BE | ts8 | bridgeKey).
func (c *Client) DeleteWebhook(ctx context.Context, id int) error {
	_, err := c.do(ctx, http.MethodDelete, "/webhooks/"+strconv.Itoa(id), nil, c.signedHeaders(be64(uint64(id))))
	return err
}

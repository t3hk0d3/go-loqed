package bridge_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/t3hk0d3/go-loqed/bridge"
)

// From HA core tests/components/loqed/fixtures/get_all_webhooks.json.
const webhooksJSON = `[{"id":1,"url":"http://10.10.10.10:8123/api/webhook/Webhook_id",
 "trigger_state_changed_open":1,"trigger_state_changed_latch":1,"trigger_state_changed_night_lock":1,
 "trigger_state_changed_unknown":1,"trigger_state_goto_open":1,"trigger_state_goto_latch":1,
 "trigger_state_goto_night_lock":1,"trigger_battery":1,"trigger_online_status":1}]`

func TestListWebhooks(t *testing.T) {
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/webhooks" {
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("Timestamp") != "1700000000" {
			t.Errorf("timestamp %q", r.Header.Get("Timestamp"))
		}
		if got := r.Header.Get("Hash"); got != "16b91b7f340c0cd4e72cf5d2405d530b4a7ca7ddc414cfee98887b9518e0598c" {
			t.Errorf("hash %q", got)
		}
		_, _ = w.Write([]byte(webhooksJSON))
	}))
	hooks, err := c.ListWebhooks(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(hooks) != 1 || hooks[0].ID != 1 || hooks[0].URL != "http://10.10.10.10:8123/api/webhook/Webhook_id" {
		t.Fatalf("got %+v", hooks)
	}
}

func TestCreateWebhook(t *testing.T) {
	const url = "http://10.0.0.5:8099/webhook/lock1"
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/webhooks" {
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Hash"); got != "8af7db84068792277430e940898c4c367655d52e74085b9901bd3e99c1dc41a4" {
			t.Errorf("hash %q", got)
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode: %v", err) // t.Fatal must not run on the handler goroutine
			return
		}
		if body["url"] != url {
			t.Errorf("url %v", body["url"])
		}
		for _, k := range []string{"trigger_state_changed_open", "trigger_state_changed_latch", "trigger_state_changed_night_lock",
			"trigger_state_changed_unknown", "trigger_state_goto_open", "trigger_state_goto_latch",
			"trigger_state_goto_night_lock", "trigger_battery", "trigger_online_status"} {
			if body[k] != float64(1) {
				t.Errorf("%s = %v", k, body[k])
			}
		}
	}))
	if err := c.CreateWebhook(context.Background(), url, bridge.AllTriggers); err != nil {
		t.Fatal(err)
	}
}

func TestCreateWebhookPartialTriggers(t *testing.T) {
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["trigger_battery"] != float64(1) || body["trigger_state_changed_open"] != float64(0) {
			t.Errorf("body %v", body)
		}
	}))
	if err := c.CreateWebhook(context.Background(), "http://x/y", bridge.TriggerBattery); err != nil {
		t.Fatal(err)
	}
}

func TestDeleteWebhook(t *testing.T) {
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete || r.URL.Path != "/webhooks/7" {
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Hash"); got != "45cf08fab64cdf2595f7ff647f8893ffb4389846defa8e9e28762938bf97489d" {
			t.Errorf("hash %q", got)
		}
	}))
	if err := c.DeleteWebhook(context.Background(), 7); err != nil {
		t.Fatal(err)
	}
}

func TestCreateWebhookBodyMatchesPython(t *testing.T) {
	const url = "http://10.0.0.5:8099/webhook/a&b<c>"
	var raw string
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		raw = string(b)
	}))
	// Stray bits above 511 must not reach the hash or the body.
	if err := c.CreateWebhook(context.Background(), url, bridge.AllTriggers|1<<12); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(raw, `{"url":"`+url+`",`) || strings.HasSuffix(raw, "\n") {
		t.Fatalf("body %q", raw)
	}
	var hash string
	c = newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hash = r.Header.Get("Hash") }))
	if err := c.CreateWebhook(context.Background(), "http://10.0.0.5:8099/webhook/lock1", bridge.AllTriggers|1<<12); err != nil {
		t.Fatal(err)
	}
	if hash != "8af7db84068792277430e940898c4c367655d52e74085b9901bd3e99c1dc41a4" {
		t.Fatalf("stray trigger bits changed the hash: %s", hash)
	}
}

type captureRT struct{ header http.Header }

func (c *captureRT) RoundTrip(r *http.Request) (*http.Response, error) {
	c.header = r.Header.Clone()
	return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("[]")), Header: http.Header{}, Request: r}, nil
}

func TestWebhookHeadersKeepUpperCaseNames(t *testing.T) {
	rt := &captureRT{}
	c, err := bridge.New("192.0.2.1:8080", bridge.Credentials{BridgeKey: testBridgeKey},
		bridge.WithHTTPClient(&http.Client{Transport: rt}),
		bridge.WithClock(func() time.Time { return fixedNow }))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.ListWebhooks(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, ok := rt.header["TIMESTAMP"]; !ok { //nolint:staticcheck // SA1008: the bridge needs the name verbatim
		t.Errorf("TIMESTAMP header not sent verbatim: %v", rt.header)
	}
	if _, ok := rt.header["HASH"]; !ok { //nolint:staticcheck // SA1008: the bridge needs the name verbatim
		t.Errorf("HASH header not sent verbatim: %v", rt.header)
	}
}

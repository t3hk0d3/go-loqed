package webhook_test

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/t3hk0d3/go-loqed/bridge"
	"github.com/t3hk0d3/go-loqed/cloud"
	"github.com/t3hk0d3/go-loqed/internal/gateway"
	"github.com/t3hk0d3/go-loqed/internal/model"
	"github.com/t3hk0d3/go-loqed/internal/webhook"
)

const secret = "0123456789abcdef0123456789abcdef"

var now = time.Unix(1700000000, 0)

type fakeSink struct {
	bridgeEvents []bridge.Event
	cloudEvents  []cloud.WebhookEvent
	busy         bool
}

func (f *fakeSink) BridgeKey(id string) ([]byte, bool) {
	if id != "lock1" {
		return nil, false
	}
	return []byte("bonjour monde"), true
}

func (f *fakeSink) DeliverBridgeEvent(_ string, ev bridge.Event) error {
	if f.busy {
		return gateway.ErrBusy
	}
	f.bridgeEvents = append(f.bridgeEvents, ev)
	return nil
}

func (f *fakeSink) DeliverCloudEvent(ev cloud.WebhookEvent) error {
	if ev.LockID != "lock1" {
		return gateway.ErrUnknownLock
	}
	f.cloudEvents = append(f.cloudEvents, ev)
	return nil
}

func (f *fakeSink) Health() map[string]gateway.Health {
	return map[string]gateway.Health{"lock1": {Mode: model.ModeLocal, Available: true}}
}

func handler(sink *fakeSink, mqttDown time.Duration, cloudSecret string) http.Handler {
	return webhook.NewHandler(webhook.Options{Sink: sink, CloudSecret: cloudSecret,
		MQTTDownFor: func() time.Duration { return mqttDown },
		Now:         func() time.Time { return now }, Log: slog.New(slog.DiscardHandler)})
}

func signed(body string, ts int64) (string, string) {
	h := sha256.New()
	h.Write([]byte(body))
	h.Write(binary.BigEndian.AppendUint64(nil, uint64(ts)))
	h.Write([]byte("bonjour monde"))
	return hex.EncodeToString(h.Sum(nil)), strconv.FormatInt(ts, 10)
}

// post sends a request through the handler. A real server canonicalizes
// incoming header names (the bridge sends TIMESTAMP/HASH), so set them
// canonically here too.
func post(h http.Handler, path, body string, header map[string]string) int {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	for k, v := range header {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec.Code
}

const reached = `{"requested_state":"NIGHT_LOCK","event_type":"STATE_CHANGED_NIGHT_LOCK","key_local_id":255}`

func TestBridgeWebhook(t *testing.T) {
	sink := &fakeSink{}
	h := handler(sink, 0, "")
	hash, ts := signed(reached, now.Unix())
	if code := post(h, "/webhook/lock1", reached, map[string]string{"HASH": hash, "TIMESTAMP": ts}); code != 200 {
		t.Fatalf("code %d", code)
	}
	if len(sink.bridgeEvents) != 1 {
		t.Fatal("event not delivered")
	}
}

func TestBridgeWebhookErrors(t *testing.T) {
	hash, ts := signed(reached, now.Unix())
	staleHash, staleTS := signed(reached, now.Unix()-60)
	cases := []struct {
		name   string
		path   string
		header map[string]string
		busy   bool
		want   int
	}{
		{"missing headers", "/webhook/lock1", nil, false, 400},
		{"unknown lock", "/webhook/other", map[string]string{"HASH": hash, "TIMESTAMP": ts}, false, 404},
		{"bad hash", "/webhook/lock1", map[string]string{"HASH": strings.Repeat("0", 64), "TIMESTAMP": ts}, false, 401},
		{"stale", "/webhook/lock1", map[string]string{"HASH": staleHash, "TIMESTAMP": staleTS}, false, 401},
		{"busy", "/webhook/lock1", map[string]string{"HASH": hash, "TIMESTAMP": ts}, true, 503},
	}
	for _, c := range cases {
		sink := &fakeSink{busy: c.busy}
		if code := post(handler(sink, 0, ""), c.path, reached, c.header); code != c.want {
			t.Errorf("%s: got %d want %d", c.name, code, c.want)
		}
		if c.want != 200 && len(sink.bridgeEvents) != 0 {
			t.Errorf("%s: rejected event was delivered", c.name)
		}
	}
}

func TestBridgeWebhookBodyLimit(t *testing.T) {
	big := strings.Repeat("x", 70<<10)
	hash, ts := signed(big, now.Unix())
	if code := post(handler(&fakeSink{}, 0, ""), "/webhook/lock1", big, map[string]string{"HASH": hash, "TIMESTAMP": ts}); code != 413 {
		t.Fatalf("code %d", code)
	}
}

func TestCloudWebhook(t *testing.T) {
	sink := &fakeSink{}
	h := handler(sink, 0, secret)
	body := `{"requested_state":"DAY_LOCK","event_type":"STATE_CHANGED_LATCH","lock_id":"lock1","key_name_user":"Front door"}`
	if code := post(h, "/cloud/"+secret, body, nil); code != 200 || len(sink.cloudEvents) != 1 {
		t.Fatalf("code %d events %d", code, len(sink.cloudEvents))
	}
	if code := post(h, "/cloud/wrong-secret-0000000000000000", body, nil); code != 404 {
		t.Fatalf("wrong secret: %d", code)
	}
	if code := post(h, "/cloud/"+secret, `{"lock_id":"other","online":1}`, nil); code != 404 {
		t.Fatalf("unknown lock: %d", code)
	}
	if code := post(h, "/cloud/"+secret, `garbage`, nil); code != 400 {
		t.Fatalf("garbage: %d", code)
	}
	if code := post(handler(sink, 0, ""), "/cloud/"+secret, body, nil); code != 404 {
		t.Fatalf("cloud route must be off without a secret: %d", code)
	}
}

func TestHealthzToleratesShortMQTTOutages(t *testing.T) {
	cases := []struct {
		down time.Duration
		code int
		conn bool
	}{
		{0, 200, true},
		{2 * time.Minute, 200, false}, // broker restart: no watchdog restart
		{6 * time.Minute, 503, false},
	}
	for _, c := range cases {
		rec := httptest.NewRecorder()
		handler(&fakeSink{}, c.down, "").ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
		var body webhook.HealthReport
		if rec.Code != c.code || json.Unmarshal(rec.Body.Bytes(), &body) != nil ||
			body.MQTTConnected != c.conn || body.Locks["lock1"].Mode != model.ModeLocal {
			t.Fatalf("down=%v code %d body %s", c.down, rec.Code, rec.Body)
		}
	}
}

func TestURLs(t *testing.T) {
	u, err := webhook.PrivateURL("http://gw.lan:8099/", 8099, "lock 1", "192.0.2.10")
	if err != nil || u != "http://gw.lan:8099/webhook/lock%201" {
		t.Fatalf("%q %v", u, err)
	}
	u, err = webhook.PrivateURL("", 8099, "lock1", "127.0.0.1")
	if err != nil || u != "http://127.0.0.1:8099/webhook/lock1" {
		t.Fatalf("%q %v", u, err)
	}
	if got := webhook.CloudURL("https://loqed.example.com/", secret); got != "https://loqed.example.com/cloud/"+secret {
		t.Fatal(got)
	}
}

func TestLikelyContainerAddress(t *testing.T) {
	if !webhook.LikelyContainerAddress(net.ParseIP("172.17.0.2"), "192.168.1.50") {
		t.Fatal("docker bridge address towards a LAN bridge")
	}
	if webhook.LikelyContainerAddress(net.ParseIP("192.168.1.10"), "192.168.1.50") ||
		webhook.LikelyContainerAddress(net.ParseIP("172.20.0.5"), "172.20.0.9:80") {
		t.Fatal("false positive")
	}
}

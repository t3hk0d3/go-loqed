package app_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/t3hk0d3/go-loqed/internal/app"
	"github.com/t3hk0d3/go-loqed/internal/model"
	"github.com/t3hk0d3/go-loqed/internal/testutil"
)

// logBuffer is a goroutine-safe log sink.
type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *logBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *logBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type relay struct {
	sub   *testutil.Subscriber
	addr  string
	logs  *logBuffer
	hooks func() []string
}

// startRelay runs one lock in local mode with mqtt.cloud_webhooks on and no
// public_url, and waits until the gateway listens on its cloud_webhook topic.
func startRelay(t *testing.T) *relay {
	t.Helper()
	broker := testutil.StartBroker(t)
	sub := testutil.Subscribe(t, broker, "loqed/#")
	fb := &fakeBridge{bolt: "day_lock"}
	bridgeSrv := httptest.NewServer(fb)
	t.Cleanup(bridgeSrv.Close)
	var calls atomic.Int32
	cloudSrv := fakeCloud(t, strings.TrimPrefix(bridgeSrv.URL, "http://"), &calls)
	cfg := baseConfig(filepath.Join(t.TempDir(), "locks.json"), broker)
	cfg.MQTT.ClientID = "gw-" + t.Name()
	cfg.MQTT.CloudWebhooks = true
	logs := &logBuffer{}
	addr, _ := startWith(t, cfg, cloudSrv.URL, func(o *app.Options) {
		o.Log = slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	})
	// "online" follows the client's subscriptions.
	sub.WaitFor(t, 10*time.Second, func(m testutil.Message) bool { return m.Topic == "loqed/status" && string(m.Payload) == "online" })
	eventually(t, "webhook registration", func() bool {
		hooks, _ := fb.snapshot()
		return len(hooks) == 1
	})
	sub.WaitFor(t, 10*time.Second, func(m testutil.Message) bool {
		var s model.State
		return m.Topic == "loqed/lock1/state" && json.Unmarshal(m.Payload, &s) == nil && s.Mode == model.ModeLocal
	})
	return &relay{sub: sub, addr: addr, logs: logs, hooks: func() []string { h, _ := fb.snapshot(); return h }}
}

func (r *relay) events(eventType model.EventType) []model.Event {
	var out []model.Event
	for _, m := range r.sub.Messages() {
		var e model.Event
		if m.Topic == "loqed/lock1/event" && json.Unmarshal(m.Payload, &e) == nil && e.EventType == eventType {
			out = append(out, e)
		}
	}
	return out
}

func (r *relay) waitLog(t *testing.T, parts ...string) {
	t.Helper()
	eventually(t, "log line with "+strings.Join(parts, ", "), func() bool {
		for line := range strings.Lines(r.logs.String()) {
			all := true
			for _, p := range parts {
				all = all && strings.Contains(line, p)
			}
			if all {
				return true
			}
		}
		return false
	})
}

const (
	relayLocked   = `{"event_type":"STATE_CHANGED_NIGHT_LOCK","requested_state":"NIGHT_LOCK","lock_id":6148,"key_local_id":""}`
	relayUnlocked = `{"event_type":"STATE_CHANGED_LATCH","requested_state":"DAY_LOCK","lock_id":6148,"key_local_id":"1"}`
)

func TestCloudWebhookOverMQTTApplied(t *testing.T) {
	r := startRelay(t)
	r.sub.Publish(t, "loqed/lock1/cloud_webhook", relayLocked, false)
	r.sub.WaitFor(t, 10*time.Second, func(m testutil.Message) bool {
		var s model.State
		return m.Topic == "loqed/lock1/state" && json.Unmarshal(m.Payload, &s) == nil && s.Lock != nil && *s.Lock == model.Locked
	})
	eventually(t, "locked event", func() bool { return len(r.events(model.EventLocked)) == 1 })
	if e := r.events(model.EventLocked)[0]; e.Source != nil {
		t.Fatalf("a keyless event has no source: %+v", e)
	}
	if n := strings.Count(r.logs.String(), "topic=loqed/lock1/cloud_webhook"); n != 1 {
		t.Fatalf("the lock's cloud_webhook topic must be logged once at startup, got %d:\n%s", n, r.logs.String())
	}
	// No public_url: the HTTP cloud route stays unrouted.
	resp, err := http.Post("http://"+r.addr+"/cloud/0123456789abcdef0123456789abcdef/lock1", "application/json", strings.NewReader(relayLocked))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("cloud route without public_url: %d", resp.StatusCode)
	}
}

func TestCloudWebhookOverMQTTDeduplicated(t *testing.T) {
	r := startRelay(t)
	// QoS 1 redelivery: the same body twice.
	r.sub.Publish(t, "loqed/lock1/cloud_webhook", relayLocked, false)
	r.sub.Publish(t, "loqed/lock1/cloud_webhook", relayLocked, false)
	eventually(t, "locked event", func() bool { return len(r.events(model.EventLocked)) >= 1 })

	// The same event from the bridge and from the relay.
	postSigned(r.hooks()[0], `{"requested_state":"DAY_LOCK","event_type":"STATE_CHANGED_LATCH","key_local_id":1,"mac_wifi":"aa","mac_ble":"bb"}`)
	r.sub.Publish(t, "loqed/lock1/cloud_webhook", relayUnlocked, false)
	eventually(t, "unlocked event", func() bool { return len(r.events(model.EventUnlocked)) >= 1 })

	time.Sleep(500 * time.Millisecond)
	if n, m := len(r.events(model.EventLocked)), len(r.events(model.EventUnlocked)); n != 1 || m != 1 {
		t.Fatalf("published %d locked and %d unlocked events, want one each", n, m)
	}
}

// A shared relay automation posting one lock's events to another lock's topic.
func TestCloudWebhookOverMQTTMismatchDropped(t *testing.T) {
	r := startRelay(t)
	r.sub.Publish(t, "loqed/lock1/cloud_webhook", relayLocked, false)
	eventually(t, "locked event", func() bool { return len(r.events(model.EventLocked)) == 1 })
	r.sub.Publish(t, "loqed/lock1/cloud_webhook", strings.Replace(relayUnlocked, "6148", "7001", 1), false)
	r.waitLog(t, "level=WARN", "lock_id=lock1", " cloud_lock_id=7001", "bound_cloud_lock_id=6148")
	time.Sleep(300 * time.Millisecond)
	if n := len(r.events(model.EventUnlocked)); n != 0 {
		t.Fatalf("event from another lock published %d times", n)
	}
	var last model.State
	for _, m := range r.sub.Messages() {
		if m.Topic == "loqed/lock1/state" {
			_ = json.Unmarshal(m.Payload, &last)
		}
	}
	if last.Lock == nil || *last.Lock != model.Locked {
		t.Fatalf("state changed by another lock's event: %+v", last)
	}
}

func TestCloudWebhookOverMQTTInvalidBodyDropped(t *testing.T) {
	r := startRelay(t)
	before := len(r.sub.Messages())
	r.sub.Publish(t, "loqed/lock1/cloud_webhook", `{"lock_id":6148,"something":"else"}`, false)
	r.waitLog(t, "level=WARN", "lock_id=lock1", "invalid")
	time.Sleep(300 * time.Millisecond)
	for _, m := range r.sub.Messages()[before:] {
		if m.Topic == "loqed/lock1/event" {
			t.Fatalf("published %s", m.Payload)
		}
	}
	if strings.Contains(r.logs.String(), "something") {
		t.Fatalf("payload logged:\n%s", r.logs.String())
	}
}

// The documented relay forwards LOQED's body as-is, personal fields included.
func TestCloudWebhookOverMQTTNeverLogsPayload(t *testing.T) {
	r := startRelay(t)
	body := `{"event_type":"STATE_CHANGED_NIGHT_LOCK","requested_state":"NIGHT_LOCK","lock_id":6148,"key_local_id":"1",` +
		`"key_account_email":"jane.doe@example.com","key_account_name":"Jane Doe","key_name_admin":"Admin Jane",` +
		`"value1":"v1-jane","value2":"v2-private","value3":"v3-private"}`
	r.sub.Publish(t, "loqed/lock1/cloud_webhook", body, false)
	eventually(t, "locked event", func() bool { return len(r.events(model.EventLocked)) == 1 })
	// A mismatch and an invalid body log too; neither may carry the payload.
	r.sub.Publish(t, "loqed/lock1/cloud_webhook", strings.Replace(body, "6148", "7001", 1), false)
	r.sub.Publish(t, "loqed/lock1/cloud_webhook", strings.Replace(body, `"event_type":"STATE_CHANGED_NIGHT_LOCK",`, "", 1), false)
	r.waitLog(t, "cloud_lock_id=7001")
	time.Sleep(300 * time.Millisecond)
	out := r.logs.String()
	for _, secret := range []string{"jane", "Jane", "v2-private", "v3-private", "v1-jane"} {
		if strings.Contains(out, secret) {
			t.Fatalf("log contains %q:\n%s", secret, out)
		}
	}
}

package mqtt_test

import (
	"bytes"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/t3hk0d3/go-loqed/internal/mqtt"
	"github.com/t3hk0d3/go-loqed/internal/testutil"
)

// marker stands for the personal data in a cloud webhook body: it must never
// reach a log line.
const marker = "jane.doe@example.com"

const cloudBody = `{"event_type":"STATE_CHANGED_NIGHT_LOCK","lock_id":6148,"key_local_id":"1","key_account_email":"` + marker + `"}`

// logBuffer is a goroutine-safe log sink for client handlers.
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

// waitLog waits until the log contains want.
func (b *logBuffer) waitLog(t *testing.T, want string) {
	t.Helper()
	deadline := time.Now().Add(wait)
	for !strings.Contains(b.String(), want) {
		if time.Now().After(deadline) {
			t.Fatalf("log never contained %q:\n%s", want, b.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (b *logBuffer) assertNoPayload(t *testing.T) {
	t.Helper()
	if out := b.String(); strings.Contains(out, marker) || strings.Contains(out, "STATE_CHANGED") {
		t.Fatalf("payload logged:\n%s", out)
	}
}

// startCloudClient starts a client for lock1 and a lock whose id is not a
// valid topic level, and waits until its subscriptions have settled.
func startCloudClient(t *testing.T, url string, enabled bool) (*mqtt.Client, *logBuffer) {
	t.Helper()
	logs := &logBuffer{}
	c := mqtt.NewClient(mqtt.ClientConfig{URL: url, ClientID: "gw-" + t.Name(), Topics: topics, CloudWebhooks: enabled},
		slog.New(slog.NewTextHandler(logs, nil)))
	c.SetLocks([]mqtt.LockInfo{{ID: "lock1"}, {ID: "Yq1g/K4"}}, nil)
	c.Start()
	t.Cleanup(c.Close)
	deadline := time.Now().Add(wait)
	for !c.Connected() {
		if time.Now().After(deadline) {
			t.Fatal("client did not connect")
		}
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond) // let the client's subscriptions settle
	return c, logs
}

func expectNoCloudWebhook(t *testing.T, c *mqtt.Client) {
	t.Helper()
	select {
	case cw := <-c.CloudWebhooks():
		t.Fatalf("unexpected cloud webhook for %s", cw.LockID)
	case <-time.After(300 * time.Millisecond):
	}
}

func TestCloudWebhookDeliveredWithRealLockID(t *testing.T) {
	url := testutil.StartBroker(t)
	c, logs := startCloudClient(t, url, true)
	pub := testutil.Subscribe(t, url, "unused/#")
	pub.Publish(t, "loqed/Yq1g_K4/cloud_webhook", cloudBody, false)
	select {
	case cw := <-c.CloudWebhooks():
		if cw.LockID != "Yq1g/K4" || string(cw.Body) != cloudBody {
			t.Fatalf("got %s %q", cw.LockID, cw.Body)
		}
	case <-time.After(wait):
		t.Fatal("cloud webhook not delivered")
	}
	logs.assertNoPayload(t)
}

func TestCloudWebhookNotSubscribedWhenDisabled(t *testing.T) {
	url := testutil.StartBroker(t)
	c, logs := startCloudClient(t, url, false)
	pub := testutil.Subscribe(t, url, "unused/#")
	pub.Publish(t, "loqed/lock1/cloud_webhook", cloudBody, false)
	expectNoCloudWebhook(t, c)
	logs.assertNoPayload(t)
}

// A relay configured with retain: true leaves its last body on the broker,
// which would replay it on every (re)connect.
func TestCloudWebhookRetainedIgnored(t *testing.T) {
	url := testutil.StartBroker(t)
	pub := testutil.Subscribe(t, url, "loqed/status")
	pub.Publish(t, "loqed/lock1/cloud_webhook", cloudBody, true)
	c, logs := startCloudClient(t, url, true)
	logs.waitLog(t, "without the retain flag")
	expectNoCloudWebhook(t, c)

	online := func(m testutil.Message) bool { return string(m.Payload) == "online" && !m.Retained }
	before := pub.Count(online)
	testutil.Kick(t, url, "gw-"+t.Name())
	deadline := time.Now().Add(3 * wait)
	for pub.Count(online) <= before || !c.Connected() {
		if time.Now().After(deadline) {
			t.Fatal("client did not reconnect")
		}
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(300 * time.Millisecond)
	if n := strings.Count(logs.String(), "without the retain flag"); n < 2 {
		t.Fatalf("retained message not seen again after reconnect (%d warnings)", n)
	}
	expectNoCloudWebhook(t, c)
	if !strings.Contains(logs.String(), "topic=loqed/lock1/cloud_webhook") {
		t.Fatalf("warning must name the topic:\n%s", logs.String())
	}
	logs.assertNoPayload(t)
}

func TestCloudWebhookUnknownLockIgnored(t *testing.T) {
	url := testutil.StartBroker(t)
	c, logs := startCloudClient(t, url, true)
	pub := testutil.Subscribe(t, url, "unused/#")
	pub.Publish(t, "loqed/nope/cloud_webhook", cloudBody, false)
	logs.waitLog(t, "topic=loqed/nope/cloud_webhook")
	expectNoCloudWebhook(t, c)
	logs.assertNoPayload(t)
}

func TestCloudWebhookOversizedDropped(t *testing.T) {
	url := testutil.StartBroker(t)
	c, logs := startCloudClient(t, url, true)
	pub := testutil.Subscribe(t, url, "unused/#")
	big := cloudBody + strings.Repeat(" ", 64<<10)
	pub.Publish(t, "loqed/lock1/cloud_webhook", big, false)
	logs.waitLog(t, "size="+strconv.Itoa(len(big)))
	expectNoCloudWebhook(t, c)
	logs.assertNoPayload(t)
}

func TestCloudWebhookAtSizeLimitDelivered(t *testing.T) {
	url := testutil.StartBroker(t)
	c, _ := startCloudClient(t, url, true)
	pub := testutil.Subscribe(t, url, "unused/#")
	body := cloudBody + strings.Repeat(" ", 64<<10-len(cloudBody))
	pub.Publish(t, "loqed/lock1/cloud_webhook", body, false)
	select {
	case cw := <-c.CloudWebhooks():
		if len(cw.Body) != 64<<10 {
			t.Fatalf("body size %d", len(cw.Body))
		}
	case <-time.After(wait):
		t.Fatal("cloud webhook at the limit not delivered")
	}
}

func TestCloudWebhookQueueFullDrops(t *testing.T) {
	url := testutil.StartBroker(t)
	c, logs := startCloudClient(t, url, true)
	pub := testutil.Subscribe(t, url, "unused/#")
	for range 20 {
		pub.Publish(t, "loqed/lock1/cloud_webhook", cloudBody, false)
	}
	logs.waitLog(t, "queue full")
	if !strings.Contains(logs.String(), "lock_id=lock1") {
		t.Fatalf("warning must name the lock:\n%s", logs.String())
	}
	if n := len(c.CloudWebhooks()); n != cap(c.CloudWebhooks()) {
		t.Fatalf("queued %d of %d", n, cap(c.CloudWebhooks()))
	}
	logs.assertNoPayload(t)
}

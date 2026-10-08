package mqtt_test

import (
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/t3hk0d3/go-loqed/internal/model"
	"github.com/t3hk0d3/go-loqed/internal/mqtt"
	"github.com/t3hk0d3/go-loqed/internal/testutil"
)

const setBody = `{"revision":"9f2c41d0a1b2c3d4","webhooks":[{"url":"http://secret.example/path"}]}`

// startWebhooksClient starts a client for lock1 and a lock whose id is not a
// valid topic level, with SetWebhooks enabled or not.
func startWebhooksClient(t *testing.T, url string, enabled bool) (*mqtt.Client, *logBuffer) {
	t.Helper()
	logs := &logBuffer{}
	c := mqtt.NewClient(mqtt.ClientConfig{URL: url, ClientID: "gw-" + t.Name(), Topics: topics, BridgeWebhookControl: enabled},
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

func expectNoWebhooksRequest(t *testing.T, c *mqtt.Client) {
	t.Helper()
	select {
	case r := <-c.WebhooksRequests():
		t.Fatalf("unexpected SetWebhooks request for %s", r.LockID)
	case <-time.After(300 * time.Millisecond):
	}
}

func assertNoURL(t *testing.T, logs *logBuffer) {
	t.Helper()
	if strings.Contains(logs.String(), "secret.example") {
		t.Fatalf("request body logged:\n%s", logs.String())
	}
}

func TestWebhookListIsRetainedAndRepublished(t *testing.T) {
	url := testutil.StartBroker(t)
	c, _ := startWebhooksClient(t, url, false)
	l := model.WebhookList{Revision: "abc", Count: 1, Webhooks: []model.WebhookEntry{{ID: 7, URL: "http://g/webhook/lock1", Triggers: []string{"all"}, Gateway: true}}}
	if err := c.PublishWebhooks("Yq1g/K4", l); err != nil {
		t.Fatal(err)
	}
	sub := testutil.Subscribe(t, url, "loqed/Yq1g_K4/webhooks")
	m := sub.WaitFor(t, wait, testutil.Topic("loqed/Yq1g_K4/webhooks"))
	var got model.WebhookList
	if err := json.Unmarshal(m.Payload, &got); err != nil || !m.Retained || got.Revision != "abc" || len(got.Webhooks) != 1 {
		t.Fatalf("got %s retained=%v err=%v", m.Payload, m.Retained, err)
	}
	testutil.Kick(t, url, "gw-"+t.Name())
	sub.WaitFor(t, 3*wait, func(m testutil.Message) bool { return m.Topic == "loqed/Yq1g_K4/webhooks" && !m.Retained })
}

func TestWebhooksResultIsNotRetained(t *testing.T) {
	url := testutil.StartBroker(t)
	c, _ := startWebhooksClient(t, url, true)
	sub := testutil.Subscribe(t, url, "loqed/lock1/webhooks/result")
	if err := c.PublishWebhooksResult("lock1", model.WebhooksResult{RequestID: model.Ptr("r1"), Status: model.WebhooksOK}); err != nil {
		t.Fatal(err)
	}
	m := sub.WaitFor(t, wait, testutil.Topic("loqed/lock1/webhooks/result"))
	if m.Retained || !strings.Contains(string(m.Payload), `"request_id":"r1"`) {
		t.Fatalf("got %s retained=%v", m.Payload, m.Retained)
	}
	late := testutil.Subscribe(t, url, "loqed/lock1/webhooks/result")
	time.Sleep(300 * time.Millisecond)
	if n := late.Count(testutil.Topic("loqed/lock1/webhooks/result")); n != 0 {
		t.Fatalf("a late subscriber got %d results", n)
	}
}

func TestRemovedLockWebhookListIsCleared(t *testing.T) {
	url := testutil.StartBroker(t)
	sub := testutil.Subscribe(t, url, "#")
	startClient(t, url, fakeDiscovery{})
	sub.WaitFor(t, wait, func(m testutil.Message) bool { return m.Topic == "loqed/gone/webhooks" && len(m.Payload) == 0 })
}

func TestWebhooksRequestDeliveredWithRealLockID(t *testing.T) {
	url := testutil.StartBroker(t)
	c, logs := startWebhooksClient(t, url, true)
	pub := testutil.Subscribe(t, url, "unused/#")
	pub.Publish(t, "loqed/Yq1g_K4/webhooks/set", setBody, false)
	select {
	case r := <-c.WebhooksRequests():
		if r.LockID != "Yq1g/K4" || string(r.Body) != setBody {
			t.Fatalf("got %s %q", r.LockID, r.Body)
		}
	case <-time.After(wait):
		t.Fatal("request not delivered")
	}
	assertNoURL(t, logs)
}

func TestWebhooksRequestNotSubscribedWhenDisabled(t *testing.T) {
	url := testutil.StartBroker(t)
	c, _ := startWebhooksClient(t, url, false)
	pub := testutil.Subscribe(t, url, "unused/#")
	pub.Publish(t, "loqed/lock1/webhooks/set", setBody, false)
	expectNoWebhooksRequest(t, c)
}

func TestRetainedWebhooksRequestIgnored(t *testing.T) {
	url := testutil.StartBroker(t)
	pub := testutil.Subscribe(t, url, "unused/#")
	pub.Publish(t, "loqed/lock1/webhooks/set", setBody, true)
	c, logs := startWebhooksClient(t, url, true)
	logs.waitLog(t, "without the retain flag")
	expectNoWebhooksRequest(t, c)
	if !strings.Contains(logs.String(), "topic=loqed/lock1/webhooks/set") {
		t.Fatalf("warning must name the topic:\n%s", logs.String())
	}
	assertNoURL(t, logs)
}

func TestWebhooksRequestForUnknownLockIgnored(t *testing.T) {
	url := testutil.StartBroker(t)
	c, logs := startWebhooksClient(t, url, true)
	pub := testutil.Subscribe(t, url, "unused/#")
	pub.Publish(t, "loqed/nope/webhooks/set", setBody, false)
	logs.waitLog(t, "topic=loqed/nope/webhooks/set")
	expectNoWebhooksRequest(t, c)
}

func TestWebhooksRequestQueueFullDrops(t *testing.T) {
	url := testutil.StartBroker(t)
	c, logs := startWebhooksClient(t, url, true)
	pub := testutil.Subscribe(t, url, "unused/#")
	for range 20 {
		pub.Publish(t, "loqed/lock1/webhooks/set", setBody, false)
	}
	logs.waitLog(t, "queue full")
	if n := len(c.WebhooksRequests()); n != cap(c.WebhooksRequests()) {
		t.Fatalf("queued %d of %d", n, cap(c.WebhooksRequests()))
	}
	assertNoURL(t, logs)
}

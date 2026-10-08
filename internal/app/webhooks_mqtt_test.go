package app_test

import (
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/t3hk0d3/go-loqed/internal/app"
	"github.com/t3hk0d3/go-loqed/internal/model"
	"github.com/t3hk0d3/go-loqed/internal/testutil"
)

func lastWebhookList(sub *testutil.Subscriber) (model.WebhookList, bool) {
	var l model.WebhookList
	found := false
	for _, m := range sub.Messages() {
		if m.Topic == "loqed/lock1/webhooks" && json.Unmarshal(m.Payload, &l) == nil {
			found = true
		}
	}
	return l, found
}

func TestSetWebhooksOverMQTT(t *testing.T) {
	broker := testutil.StartBroker(t)
	sub := testutil.Subscribe(t, broker, "loqed/#")
	fb := &fakeBridge{bolt: "day_lock", webhooks: []string{"http://ha.local/api/webhook/keep", "http://old.local/gone"}, ids: []int{3, 5}}
	bridgeSrv := httptest.NewServer(fb)
	t.Cleanup(bridgeSrv.Close)
	var calls atomic.Int32
	cloudSrv := fakeCloud(t, strings.TrimPrefix(bridgeSrv.URL, "http://"), &calls)
	cfg := baseConfig(filepath.Join(t.TempDir(), "locks.json"), broker)
	cfg.MQTT.ClientID = "gw-" + t.Name()
	cfg.MQTT.BridgeWebhookControl = true
	startWith(t, cfg, cloudSrv.URL, func(*app.Options) {})

	var list model.WebhookList
	eventually(t, "the webhook list with the gateway's own webhook", func() bool {
		l, ok := lastWebhookList(sub)
		list = l
		return ok && l.Count == 3
	})
	own := list.Webhooks[2]
	if !own.Gateway || list.Webhooks[0].ID != 3 || list.Webhooks[1].ID != 5 || !slices.Equal(list.Webhooks[0].Triggers, []string{"all"}) {
		t.Fatalf("list %+v", list)
	}
	sub.WaitFor(t, 10*time.Second, func(m testutil.Message) bool { return m.Topic == "loqed/status" && string(m.Payload) == "online" })
	time.Sleep(200 * time.Millisecond) // let the client's subscriptions settle

	sub.Publish(t, "loqed/lock1/webhooks/set",
		`{"revision":"`+list.Revision+`","request_id":"t1","webhooks":[{"id":3},{"url":"http://new.local/hook"}]}`, false)
	m := sub.WaitFor(t, 10*time.Second, testutil.Topic("loqed/lock1/webhooks/result"))
	var r model.WebhooksResult
	if err := json.Unmarshal(m.Payload, &r); err != nil || m.Retained {
		t.Fatalf("result %s retained=%v err=%v", m.Payload, m.Retained, err)
	}
	if r.Status != model.WebhooksOK || r.RequestID == nil || *r.RequestID != "t1" || !slices.Equal(r.Removed, []int{5}) ||
		len(r.Added) != 1 || !slices.Equal(r.Kept, []int{3, own.ID}) || r.Revision == nil {
		t.Fatalf("result %s", m.Payload)
	}
	fb.mu.Lock()
	writes := slices.Clone(fb.writes)
	fb.mu.Unlock()
	if n := len(writes); n < 2 || writes[n-2] != "delete 5" || writes[n-1] != "create http://new.local/hook" {
		t.Fatalf("bridge writes %v", writes)
	}
	l, _ := lastWebhookList(sub)
	if l.Revision != *r.Revision || l.Count != 3 || l.Webhooks[2].ID != r.Added[0] {
		t.Fatalf("republished list %+v, result revision %s", l, *r.Revision)
	}
}

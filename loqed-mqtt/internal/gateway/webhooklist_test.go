package gateway

import (
	"slices"
	"testing"
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/bridge"
	"github.com/t3hk0d3/go-loqed/loqed-mqtt/internal/config"
	"github.com/t3hk0d3/go-loqed/loqed-mqtt/internal/model"
)

const ownURL = "http://10.0.0.5:8099/webhook/lock1"

func hook(id int, url string, t bridge.Triggers) bridge.Webhook {
	return bridge.Webhook{ID: loqed.Int(id), URL: url, Triggers: t}
}

func (h *harness) lastList() model.WebhookList {
	h.t.Helper()
	if len(h.pub.lists) == 0 {
		h.t.Fatal("no webhook list published")
	}
	return h.pub.lists[len(h.pub.lists)-1]
}

func listIDs(l model.WebhookList) []int {
	var ids []int
	for _, e := range l.Webhooks {
		ids = append(ids, e.ID)
	}
	return ids
}

func TestWebhookRevisionIgnoresOrder(t *testing.T) {
	a := []bridge.Webhook{hook(3, "http://a/", bridge.AllTriggers), hook(5, "http://b/", bridge.TriggerBattery)}
	b := []bridge.Webhook{a[1], a[0]}
	if webhookRevision(a) != webhookRevision(b) {
		t.Fatal("revision depends on order")
	}
	if r := webhookRevision(a); len(r) != 16 {
		t.Fatalf("revision %q is not 16 hex characters", r)
	}
}

func TestWebhookRevisionChangesWithAnyField(t *testing.T) {
	base := []bridge.Webhook{hook(3, "http://a/", bridge.AllTriggers)}
	for name, other := range map[string][]bridge.Webhook{
		"id":       {hook(4, "http://a/", bridge.AllTriggers)},
		"url":      {hook(3, "http://a/x", bridge.AllTriggers)},
		"triggers": {hook(3, "http://a/", bridge.TriggerBattery)},
		"added":    {base[0], hook(9, "http://b/", bridge.AllTriggers)},
	} {
		if webhookRevision(base) == webhookRevision(other) {
			t.Errorf("revision unchanged when %s changes", name)
		}
	}
}

func TestWebhookRevisionLengthPrefixesFields(t *testing.T) {
	// "1" + "2http://a" vs "12" + "http://a": only a length prefix tells them apart.
	a := []bridge.Webhook{hook(1, "2http://a", 0)}
	b := []bridge.Webhook{hook(12, "http://a", 0)}
	if webhookRevision(a) == webhookRevision(b) {
		t.Fatal("field boundaries are ambiguous")
	}
}

func TestRegistrationCheckPublishesTheListItRead(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.bridge.hooks = []bridge.Webhook{hook(7, ownURL, bridge.AllTriggers),
		hook(3, "http://192.168.2.10:8123/api/webhook/abc", bridge.TriggerBattery|bridge.TriggerOnlineStatus)}
	h.start()
	if h.bridge.listCalls != 1 || len(h.pub.lists) != 1 {
		t.Fatalf("lists read %d, published %d; want 1 and 1", h.bridge.listCalls, len(h.pub.lists))
	}
	l := h.lastList()
	if !slices.Equal(listIDs(l), []int{3, 7}) || l.Count != 2 || !l.FetchedAt.Equal(t0) || l.Revision != webhookRevision(h.bridge.hooks) {
		t.Fatalf("list %+v", l)
	}
	if l.Webhooks[0].Gateway || !l.Webhooks[1].Gateway {
		t.Errorf("gateway flags %v %v; want only the own URL", l.Webhooks[0].Gateway, l.Webhooks[1].Gateway)
	}
	if !slices.Equal(l.Webhooks[0].Triggers, []string{"battery", "online_status"}) || !slices.Equal(l.Webhooks[1].Triggers, []string{"all"}) {
		t.Errorf("triggers %v %v", l.Webhooks[0].Triggers, l.Webhooks[1].Triggers)
	}
	if l.Webhooks[0].URL != "http://192.168.2.10:8123/api/webhook/abc" {
		t.Errorf("url %q, want the full URL", l.Webhooks[0].URL)
	}
}

func TestRegistrationCheckRereadsAfterCreatingOurWebhook(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.bridge.hooks = []bridge.Webhook{hook(3, "http://a/", bridge.AllTriggers)}
	h.start()
	if h.bridge.listCalls != 2 {
		t.Fatalf("lists read %d, want 2", h.bridge.listCalls)
	}
	if l := h.lastList(); !slices.Equal(listIDs(l), []int{3, 100}) || !l.Webhooks[1].Gateway {
		t.Fatalf("list %+v", l)
	}
}

func TestRegistrationCheckRereadsAfterDeletingAStaleRegistration(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.bridge.hooks = []bridge.Webhook{hook(2, "http://10.0.0.9:8099/webhook/lock1", bridge.AllTriggers), hook(7, ownURL, bridge.AllTriggers)}
	h.start()
	if l := h.lastList(); !slices.Equal(listIDs(l), []int{7}) {
		t.Fatalf("list %v, want the post-change list", listIDs(l))
	}
}

func TestFailedRegistrationReadPublishesNoList(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.bridge.listErr = loqed.ErrNoResponse
	h.start()
	if len(h.pub.lists) != 0 {
		t.Fatalf("published %d lists", len(h.pub.lists))
	}
}

func TestStatusWithTheSameWebhookCountReadsNoList(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.bridge.hooks = []bridge.Webhook{hook(7, ownURL, bridge.AllTriggers)}
	h.start()
	status := h.bridge.statusCalls
	h.run(5 * time.Minute) // delivery unconfirmed: /status every minute
	if h.bridge.statusCalls == status || h.bridge.listCalls != 1 {
		t.Fatalf("status reads %d→%d, lists %d; want status reads and no list read", status, h.bridge.statusCalls, h.bridge.listCalls)
	}
}

func TestStatusWithAnotherWebhookCountRereadsTheList(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.bridge.hooks = []bridge.Webhook{hook(7, ownURL, bridge.AllTriggers)}
	h.start()
	h.bridge.hooks = append(h.bridge.hooks, hook(8, "http://other/", bridge.AllTriggers)) // added by another app
	h.run(time.Minute)
	if h.bridge.listCalls != 2 || !slices.Equal(listIDs(h.lastList()), []int{7, 8}) {
		t.Fatalf("lists %d, last %v", h.bridge.listCalls, listIDs(h.lastList()))
	}
	h.run(time.Minute)
	if h.bridge.listCalls != 2 {
		t.Fatalf("lists %d after the counts agree again", h.bridge.listCalls)
	}
}

func TestFailedListReadCountsAsABridgeFailure(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.bridge.hooks = []bridge.Webhook{hook(7, ownURL, bridge.AllTriggers)}
	h.start()
	h.bridge.webhooksNumber = model.Ptr(2)
	h.bridge.listErr = loqed.ErrNoResponse
	h.run(time.Minute)
	if h.s.httpFailures != 1 || len(h.pub.lists) != 1 {
		t.Fatalf("http failures %d, lists published %d", h.s.httpFailures, len(h.pub.lists))
	}
}

func TestNoListIsReadOutsideLocalMode(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.bridge.hooks = []bridge.Webhook{hook(7, ownURL, bridge.AllTriggers)}
	h.start()
	h.toCloud()
	lists, published := h.bridge.listCalls, len(h.pub.lists)
	h.bridge.webhooksNumber = model.Ptr(5)
	h.run(30 * time.Minute)
	if h.bridge.listCalls != lists || len(h.pub.lists) != published {
		t.Fatalf("lists read %d→%d, published %d→%d in cloud mode", lists, h.bridge.listCalls, published, len(h.pub.lists))
	}
}

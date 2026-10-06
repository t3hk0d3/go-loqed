package gateway

import (
	"context"
	"errors"
	"testing"
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/bridge"
	"github.com/t3hk0d3/go-loqed/cloud"
	"github.com/t3hk0d3/go-loqed/internal/config"
	"github.com/t3hk0d3/go-loqed/internal/model"
)

func TestManagerDispatch(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	m := NewManager([]*Supervisor{h.s})
	if err := m.DeliverBridgeEvent("nope", bridge.OnlineEvent{}); !errors.Is(err, ErrUnknownLock) {
		t.Fatalf("got %v", err)
	}
	if err := m.DeliverCloudEvent("lock1", cloud.WebhookEvent{LockID: "6148"}); err != nil {
		t.Fatal(err)
	}
	if err := m.DeliverCloudEvent("nope", cloud.WebhookEvent{LockID: "6148"}); !errors.Is(err, ErrUnknownLock) {
		t.Fatalf("got %v", err)
	}
	if err := m.DeliverCommand("lock1", model.CommandLock, "", h.now); err != nil {
		t.Fatal(err)
	}
	for range 62 { // queue holds 64; 2 already queued
		_ = m.DeliverCommand("lock1", model.CommandLock, "", h.now)
	}
	if err := m.DeliverCommand("lock1", model.CommandLock, "", h.now); !errors.Is(err, ErrBusy) {
		t.Fatalf("got %v", err)
	}
	key, ok := m.BridgeKey("lock1")
	if !ok || string(key) != "bonjour monde" {
		t.Fatalf("key %q %v", key, ok)
	}
	if _, ok := m.Health()["lock1"]; !ok {
		t.Fatal("health missing lock1")
	}
}

func TestManagerRemoveStopsSupervisor(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.s.d.Now = time.Now // Run uses the real ticker
	m := NewManager([]*Supervisor{h.s})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { m.Run(ctx); close(done) }()
	deadline := time.Now().Add(5 * time.Second)
	for len(m.Health()) == 1 && m.Health()["lock1"].Mode != model.ModeLocal {
		if time.Now().After(deadline) {
			t.Fatal("supervisor did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := m.Remove([]string{"lock1", "nope"}); len(got) != 1 {
		t.Fatalf("removed %v", got)
	}
	if err := m.DeliverCommand("lock1", model.CommandOpen, "", time.Now()); !errors.Is(err, ErrUnknownLock) {
		t.Fatalf("got %v", err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run must return once every supervisor stopped")
	}
}

func TestManagerBindsCloudWebhookID(t *testing.T) {
	var saved []string
	h := newHarness(t, testRecord(), config.LockSetting{}, func(d *Deps) {
		d.SaveCloudWebhookID = func(lockID, id string) error { saved = append(saved, lockID+"="+id); return nil }
	})
	m := NewManager([]*Supervisor{h.s})
	if err := m.DeliverCloudEvent("lock1", cloud.WebhookEvent{LockID: "6148"}); err != nil {
		t.Fatal(err)
	}
	if err := m.DeliverCloudEvent("lock1", cloud.WebhookEvent{LockID: "6148"}); err != nil {
		t.Fatal(err)
	}
	if err := m.DeliverCloudEvent("lock1", cloud.WebhookEvent{LockID: "7001"}); !errors.Is(err, ErrCloudIDMismatch) {
		t.Fatalf("got %v", err)
	}
	if len(saved) != 1 || saved[0] != "lock1=6148" || h.s.Record().CloudWebhookID != "6148" {
		t.Fatalf("saved %v record %+v", saved, h.s.Record())
	}
}

func TestCloudWebhookIDSurvivesRecordRefresh(t *testing.T) {
	rec := testRecord()
	rec.CloudWebhookID = "6148"
	h := newHarness(t, rec, config.LockSetting{})
	h.s.setRecord(testRecord()) // a refresh never carries the id
	if h.s.Record().CloudWebhookID != "6148" {
		t.Fatalf("record %+v", h.s.Record())
	}
}

// Real-shape cloud webhook bodies: numeric lock_id, string key_local_id.
const (
	cloudReached6148 = `{"event_type":"STATE_CHANGED_NIGHT_LOCK","requested_state":"NIGHT_LOCK","lock_id":6148,"key_local_id":"1"}`
	cloudReached7001 = `{"event_type":"STATE_CHANGED_NIGHT_LOCK","requested_state":"NIGHT_LOCK","lock_id":7001,"key_local_id":"1"}`
)

// queued drains the supervisor's queue without running it.
func queued(s *Supervisor) []any {
	var msgs []any
	for {
		select {
		case m := <-s.in:
			msgs = append(msgs, m)
		default:
			return msgs
		}
	}
}

func TestDeliverCloudWebhookValidBody(t *testing.T) {
	var saved []string
	h := newHarness(t, testRecord(), config.LockSetting{}, func(d *Deps) {
		d.SaveCloudWebhookID = func(lockID, id string) error { saved = append(saved, lockID+"="+id); return nil }
	})
	m := NewManager([]*Supervisor{h.s})
	ev, err := m.DeliverCloudWebhook("lock1", []byte(cloudReached6148))
	if err != nil || ev.LockID != "6148" {
		t.Fatalf("event %+v err %v", ev, err)
	}
	msgs := queued(h.s)
	if len(msgs) != 1 {
		t.Fatalf("queued %v", msgs)
	}
	if got, ok := msgs[0].(CloudEventMsg); !ok || got.Event.LockID != "6148" || got.Event.EventType != ev.EventType {
		t.Fatalf("queued %+v", msgs[0])
	}
	if h.s.Record().CloudWebhookID != "6148" || len(saved) != 1 || saved[0] != "lock1=6148" {
		t.Fatalf("binding: record %+v saved %v", h.s.Record(), saved)
	}
}

func TestDeliverCloudWebhookMatchingBoundID(t *testing.T) {
	rec := testRecord()
	rec.CloudWebhookID = "6148"
	h := newHarness(t, rec, config.LockSetting{})
	m := NewManager([]*Supervisor{h.s})
	if _, err := m.DeliverCloudWebhook("lock1", []byte(cloudReached6148)); err != nil {
		t.Fatal(err)
	}
	if n := len(queued(h.s)); n != 1 {
		t.Fatalf("queued %d", n)
	}
}

func TestDeliverCloudWebhookMismatch(t *testing.T) {
	rec := testRecord()
	rec.CloudWebhookID = "6148"
	h := newHarness(t, rec, config.LockSetting{})
	m := NewManager([]*Supervisor{h.s})
	ev, err := m.DeliverCloudWebhook("lock1", []byte(cloudReached7001))
	if !errors.Is(err, ErrCloudIDMismatch) {
		t.Fatalf("got %v", err)
	}
	if ev.LockID != "7001" {
		t.Fatalf("event must carry the body's lock_id: %+v", ev)
	}
	if n := len(queued(h.s)); n != 0 {
		t.Fatalf("queued %d", n)
	}
}

func TestDeliverCloudWebhookInvalidBody(t *testing.T) {
	for name, body := range map[string]string{
		"not JSON":          `garbage`,
		"no lock_id":        `{"event_type":"STATE_CHANGED_NIGHT_LOCK"}`,
		"nothing to report": `{"lock_id":6148}`,
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, testRecord(), config.LockSetting{})
			m := NewManager([]*Supervisor{h.s})
			ev, err := m.DeliverCloudWebhook("lock1", []byte(body))
			if !errors.Is(err, loqed.ErrInvalidPayload) {
				t.Fatalf("got %v", err)
			}
			if ev != (cloud.WebhookEvent{}) {
				t.Fatalf("event must be zero: %+v", ev)
			}
			if n := len(queued(h.s)); n != 0 || h.s.Record().CloudWebhookID != "" {
				t.Fatalf("queued %d record %+v", n, h.s.Record())
			}
		})
	}
}

func TestDeliverCloudWebhookUnknownLock(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	m := NewManager([]*Supervisor{h.s})
	if _, err := m.DeliverCloudWebhook("nope", []byte(cloudReached6148)); !errors.Is(err, ErrUnknownLock) {
		t.Fatalf("got %v", err)
	}
	if h.s.Record().CloudWebhookID != "" || len(queued(h.s)) != 0 {
		t.Fatalf("bound or queued on another lock: %+v", h.s.Record())
	}
}

func TestDeliverCloudWebhookFullQueue(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	m := NewManager([]*Supervisor{h.s})
	for h.s.Deliver(struct{}{}) {
	}
	if _, err := m.DeliverCloudWebhook("lock1", []byte(cloudReached6148)); !errors.Is(err, ErrBusy) {
		t.Fatalf("got %v", err)
	}
}

func TestDeliverCloudWebhookDocumentedShape(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	m := NewManager([]*Supervisor{h.s})
	body := `{"requested_state":"DAY_LOCK","event_type":"STATE_CHANGED_LATCH","lock_id":"Yq1g","key_local_id":3}`
	if _, err := m.DeliverCloudWebhook("lock1", []byte(body)); err != nil {
		t.Fatal(err)
	}
	if h.s.Record().CloudWebhookID != "Yq1g" || len(queued(h.s)) != 1 {
		t.Fatalf("record %+v", h.s.Record())
	}
}

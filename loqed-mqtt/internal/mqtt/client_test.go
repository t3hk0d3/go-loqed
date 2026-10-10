package mqtt_test

import (
	"encoding/json"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/t3hk0d3/go-loqed/loqed-mqtt/internal/model"
	"github.com/t3hk0d3/go-loqed/loqed-mqtt/internal/mqtt"
	"github.com/t3hk0d3/go-loqed/loqed-mqtt/internal/testutil"
)

const wait = 5 * time.Second

// fakeDiscovery stands in for Home Assistant: the client must only use the
// topics and payloads it is given.
type fakeDiscovery struct{}

func (fakeDiscovery) BirthTopic() string         { return "disc/status" }
func (fakeDiscovery) Topic(lockID string) string { return "disc/" + mqtt.TopicID(lockID) + "/config" }
func (fakeDiscovery) Payload(l mqtt.LockInfo) ([]byte, error) {
	return []byte(`{"id":"` + l.ID + `"}`), nil
}

func startClient(t *testing.T, url string, d mqtt.Discovery) *mqtt.Client {
	t.Helper()
	c := mqtt.NewClient(mqtt.ClientConfig{URL: url, ClientID: "gw-" + t.Name(), Topics: topics, Discovery: d},
		slog.New(slog.DiscardHandler))
	c.SetLocks([]mqtt.LockInfo{{ID: "lock1", Name: "Front door"}}, []string{"gone"})
	c.Start()
	t.Cleanup(c.Close)
	deadline := time.Now().Add(wait)
	for !c.Connected() {
		if time.Now().After(deadline) {
			t.Fatal("client did not connect")
		}
		time.Sleep(20 * time.Millisecond)
	}
	return c
}

func TestPublishesStatusDiscoveryAndRetainedState(t *testing.T) {
	url := testutil.StartBroker(t)
	c := startClient(t, url, fakeDiscovery{})
	if c.DisconnectedFor() != 0 {
		t.Fatal("connected client reports downtime")
	}
	if err := c.PublishState("lock1", model.State{Lock: model.Ptr(model.Locked), Mode: model.ModeLocal}); err != nil {
		t.Fatal(err)
	}
	if err := c.PublishAvailability("lock1", true); err != nil {
		t.Fatal(err)
	}
	// A subscriber connecting later must see retained messages.
	sub := testutil.Subscribe(t, url, "#")
	sub.WaitFor(t, wait, func(m testutil.Message) bool {
		return m.Topic == "loqed/status" && string(m.Payload) == "online" && m.Retained
	})
	sub.WaitFor(t, wait, func(m testutil.Message) bool {
		return m.Topic == "disc/lock1/config" && m.Retained && string(m.Payload) == `{"id":"lock1"}`
	})
	sub.WaitFor(t, wait, func(m testutil.Message) bool {
		return m.Topic == "loqed/lock1/availability" && string(m.Payload) == "online"
	})
	st := sub.WaitFor(t, wait, testutil.Topic("loqed/lock1/state"))
	var s model.State
	if err := json.Unmarshal(st.Payload, &s); err != nil || s.Lock == nil || *s.Lock != model.Locked {
		t.Fatalf("state %s %v", st.Payload, err)
	}
}

func TestRemovedLockTopicsAreCleared(t *testing.T) {
	url := testutil.StartBroker(t)
	sub := testutil.Subscribe(t, url, "#")
	startClient(t, url, fakeDiscovery{})
	for _, topic := range []string{"disc/gone/config", "loqed/gone/state", "loqed/gone/availability"} {
		sub.WaitFor(t, wait, func(m testutil.Message) bool { return m.Topic == topic && len(m.Payload) == 0 })
	}
}

// A retained command (e.g. published by mistake with the retain flag) is
// redelivered on every reconnect; it must never actuate the lock.
func TestRetainedCommandIsIgnored(t *testing.T) {
	url := testutil.StartBroker(t)
	pub := testutil.Subscribe(t, url, "unused/#")
	pub.Publish(t, "loqed/lock1/command", "OPEN", true)
	c := startClient(t, url, fakeDiscovery{})
	time.Sleep(300 * time.Millisecond)
	pub.Publish(t, "loqed/lock1/command", "LOCK", false)
	select {
	case cmd := <-c.Commands():
		if cmd.Command != model.CommandLock {
			t.Fatalf("retained command delivered: %+v", cmd)
		}
	case <-time.After(wait):
		t.Fatal("live command not delivered")
	}
}

func TestRetainedJSONCommandIsIgnored(t *testing.T) {
	url := testutil.StartBroker(t)
	pub := testutil.Subscribe(t, url, "unused/#")
	pub.Publish(t, "loqed/lock1/command", `{"command":"OPEN","id":"x"}`, true)
	c := startClient(t, url, fakeDiscovery{})
	time.Sleep(300 * time.Millisecond)
	pub.Publish(t, "loqed/lock1/command", `{"command":"LOCK","id":"live"}`, false)
	select {
	case cmd := <-c.Commands():
		if cmd.Command != model.CommandLock || cmd.ID != "live" {
			t.Fatalf("retained command delivered: %+v", cmd)
		}
	case <-time.After(wait):
		t.Fatal("live command not delivered")
	}
}

func TestJSONCommandCarriesID(t *testing.T) {
	url := testutil.StartBroker(t)
	c := startClient(t, url, fakeDiscovery{})
	sub := testutil.Subscribe(t, url, "unused/#")
	time.Sleep(200 * time.Millisecond)
	sub.Publish(t, "loqed/lock1/command", `{"command":"LOCK","id":"`+strings.Repeat("x", 65)+`"}`, false)
	sub.Publish(t, "loqed/lock1/command", `{"command":"UNLOCK","id":"auto-42"}`, false)
	select {
	case cmd := <-c.Commands():
		if cmd.LockID != "lock1" || cmd.Command != model.CommandUnlock || cmd.ID != "auto-42" {
			t.Fatalf("got %+v", cmd)
		}
	case <-time.After(wait):
		t.Fatal("no command")
	}
}

func TestCommandStatusIsRetainedAndRepublished(t *testing.T) {
	url := testutil.StartBroker(t)
	c := startClient(t, url, fakeDiscovery{})
	st := model.CommandStatus{Command: model.CommandLock, ID: model.Ptr("abc"), Status: model.StatusSent, Attempts: 1}
	if err := c.PublishCommandStatus("lock1", st); err != nil {
		t.Fatal(err)
	}
	late := testutil.Subscribe(t, url, "loqed/lock1/command_status")
	m := late.WaitFor(t, wait, testutil.Topic("loqed/lock1/command_status"))
	var got model.CommandStatus
	if err := json.Unmarshal(m.Payload, &got); err != nil || !m.Retained || got.Status != model.StatusSent || *got.ID != "abc" {
		t.Fatalf("got %s retained=%v err=%v", m.Payload, m.Retained, err)
	}
}

func TestRemovedLockCommandStatusIsCleared(t *testing.T) {
	url := testutil.StartBroker(t)
	sub := testutil.Subscribe(t, url, "#")
	startClient(t, url, fakeDiscovery{})
	sub.WaitFor(t, wait, func(m testutil.Message) bool { return m.Topic == "loqed/gone/command_status" && len(m.Payload) == 0 })
}

func TestBrokerAddrDropsCredentialsPathAndQuery(t *testing.T) {
	for in, want := range map[string]string{
		"tcp://user:s3cret@broker:1883":          "tcp://broker:1883",
		"wss://u:p@broker:443/mqtt?token=s3cret": "wss://broker:443",
		"ssl://broker":                           "ssl://broker",
		"::not a url":                            "<unparsable URL>",
	} {
		if got := mqtt.BrokerAddr(in); got != want {
			t.Errorf("BrokerAddr(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestDisconnectedFor(t *testing.T) {
	now := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	c := mqtt.NewClient(mqtt.ClientConfig{URL: "tcp://127.0.0.1:1", ClientID: "x", Topics: topics,
		Now: func() time.Time { return now }}, slog.New(slog.DiscardHandler))
	now = now.Add(6 * time.Minute)
	if d := c.DisconnectedFor(); d != 6*time.Minute {
		t.Fatalf("never connected: %v", d)
	}
}

func TestEventsAreNotRetained(t *testing.T) {
	url := testutil.StartBroker(t)
	sub := testutil.Subscribe(t, url, "loqed/#")
	c := startClient(t, url, fakeDiscovery{})
	if err := c.PublishEvent("lock1", model.Event{EventType: model.EventLocked, Reason: "STATE_CHANGED_NIGHT_LOCK"}); err != nil {
		t.Fatal(err)
	}
	m := sub.WaitFor(t, wait, testutil.Topic("loqed/lock1/event"))
	if m.Retained {
		t.Fatal("event must not be retained")
	}
	late := testutil.Subscribe(t, url, "loqed/lock1/event")
	time.Sleep(300 * time.Millisecond)
	if late.Count(testutil.Topic("loqed/lock1/event")) != 0 {
		t.Fatal("late subscriber must not receive old events")
	}
}

func TestCommandsAreDelivered(t *testing.T) {
	url := testutil.StartBroker(t)
	c := startClient(t, url, fakeDiscovery{})
	sub := testutil.Subscribe(t, url, "unused/#")
	time.Sleep(200 * time.Millisecond) // let the client's subscription settle
	sub.Publish(t, "loqed/lock1/command", "lock", false)
	sub.Publish(t, "loqed/unknown/command", "LOCK", false)
	sub.Publish(t, "loqed/lock1/command", "EXPLODE", false)
	select {
	case cmd := <-c.Commands():
		if cmd.LockID != "lock1" || cmd.Command != model.CommandLock || cmd.At.IsZero() {
			t.Fatalf("got %+v", cmd)
		}
	case <-time.After(wait):
		t.Fatal("no command")
	}
	select {
	case cmd := <-c.Commands():
		t.Fatalf("unexpected extra command %+v", cmd)
	case <-time.After(300 * time.Millisecond):
	}
}

func TestBirthMessageRepublishesDiscovery(t *testing.T) {
	url := testutil.StartBroker(t)
	sub := testutil.Subscribe(t, url, "disc/#")
	startClient(t, url, fakeDiscovery{})
	isDiscovery := func(m testutil.Message) bool {
		return m.Topic == "disc/lock1/config" && len(m.Payload) > 0
	}
	sub.WaitFor(t, wait, isDiscovery)
	before := sub.Count(isDiscovery)
	sub.Publish(t, "disc/status", "online", false)
	deadline := time.Now().Add(wait)
	for sub.Count(isDiscovery) <= before {
		if time.Now().After(deadline) {
			t.Fatal("discovery not republished after the birth message")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestNilDiscoveryPublishesOnlyGatewayTopics(t *testing.T) {
	url := testutil.StartBroker(t)
	sub := testutil.Subscribe(t, url, "#")
	c := startClient(t, url, nil)
	_ = c.PublishState("lock1", model.State{Mode: model.ModeLocal})
	sub.WaitFor(t, wait, testutil.Topic("loqed/lock1/state"))
	sub.WaitFor(t, wait, func(m testutil.Message) bool { return m.Topic == "loqed/gone/state" && len(m.Payload) == 0 })
	if n := sub.Count(func(m testutil.Message) bool { return !strings.HasPrefix(m.Topic, "loqed/") }); n != 0 {
		t.Fatalf("published outside the gateway's topics without discovery (%d messages)", n)
	}
}

func TestAvailabilityIsDeduplicated(t *testing.T) {
	url := testutil.StartBroker(t)
	sub := testutil.Subscribe(t, url, "loqed/lock1/availability")
	c := startClient(t, url, fakeDiscovery{})
	for range 3 {
		_ = c.PublishAvailability("lock1", true)
	}
	sub.WaitFor(t, wait, testutil.Topic("loqed/lock1/availability"))
	time.Sleep(300 * time.Millisecond)
	if n := sub.Count(testutil.Topic("loqed/lock1/availability")); n != 1 {
		t.Fatalf("availability published %d times", n)
	}
}

// Removals requested before the broker is reachable must accumulate, and a
// lock that comes back must not be cleared.
func TestPendingRemovalsSurviveSecondSetLocks(t *testing.T) {
	url := testutil.StartBroker(t)
	sub := testutil.Subscribe(t, url, "#")
	cleared := make(chan []string, 4)
	c := mqtt.NewClient(mqtt.ClientConfig{URL: url, ClientID: "gw-" + t.Name(), Topics: topics, Discovery: fakeDiscovery{},
		OnRemovedCleared: func(ids []string) { cleared <- ids }}, slog.New(slog.DiscardHandler))
	c.SetLocks([]mqtt.LockInfo{{ID: "lock1", Name: "Front door"}}, []string{"gone1", "back"})
	c.SetLocks([]mqtt.LockInfo{{ID: "lock1", Name: "Front door"}, {ID: "back", Name: "Back"}}, []string{"gone2"})
	c.Start()
	t.Cleanup(c.Close)
	for _, id := range []string{"gone1", "gone2"} {
		sub.WaitFor(t, wait, func(m testutil.Message) bool {
			return m.Topic == "disc/"+id+"/config" && len(m.Payload) == 0
		})
	}
	select {
	case ids := <-cleared:
		if len(ids) != 2 {
			t.Fatalf("cleared %v", ids)
		}
	case <-time.After(wait):
		t.Fatal("OnRemovedCleared not called")
	}
}

// Commands must reach the gateway in publish order: OPEN then LOCK must
// not become LOCK then OPEN (latest command wins).
func TestCommandsKeepPublishOrder(t *testing.T) {
	url := testutil.StartBroker(t)
	c := startClient(t, url, fakeDiscovery{})
	sub := testutil.Subscribe(t, url, "unused/#")
	time.Sleep(200 * time.Millisecond) // let the client's subscription settle
	const n = 200
	got := make(chan []string, 1)
	go func() {
		var ids []string
		for len(ids) < n {
			select {
			case cmd := <-c.Commands():
				ids = append(ids, cmd.ID)
			case <-time.After(5 * time.Second):
				got <- ids
				return
			}
		}
		got <- ids
	}()
	var want []string
	for i := range n {
		id := strconv.Itoa(i)
		want = append(want, id)
		sub.Publish(t, "loqed/lock1/command", `{"command":"LOCK","id":"`+id+`"}`, false)
	}
	if ids := <-got; !slices.Equal(ids, want) {
		t.Fatalf("commands out of order or lost:\n got  %v\n want %v", ids, want)
	}
}

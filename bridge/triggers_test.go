package bridge_test

import (
	"context"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/t3hk0d3/go-loqed/bridge"
)

var triggerNames = []struct {
	name string
	bit  bridge.Triggers
}{
	{"state_changed_open", bridge.TriggerStateChangedOpen},
	{"state_changed_latch", bridge.TriggerStateChangedLatch},
	{"state_changed_night_lock", bridge.TriggerStateChangedNightLock},
	{"state_changed_unknown", bridge.TriggerStateChangedUnknown},
	{"state_goto_open", bridge.TriggerGotoOpen},
	{"state_goto_latch", bridge.TriggerGotoLatch},
	{"state_goto_night_lock", bridge.TriggerGotoNightLock},
	{"battery", bridge.TriggerBattery},
	{"online_status", bridge.TriggerOnlineStatus},
}

func TestParseTriggersReturnsTheNamedBit(t *testing.T) {
	for _, tc := range triggerNames {
		got, err := bridge.ParseTriggers([]string{tc.name})
		if err != nil || got != tc.bit {
			t.Errorf("ParseTriggers(%q) = %v, %v; want %v", tc.name, got, err, tc.bit)
		}
	}
}

func TestParseTriggersAllIsEveryTrigger(t *testing.T) {
	got, err := bridge.ParseTriggers([]string{"all"})
	if err != nil || got != bridge.AllTriggers {
		t.Fatalf("got %v, %v", got, err)
	}
}

func TestParseTriggersAllowsDuplicates(t *testing.T) {
	got, err := bridge.ParseTriggers([]string{"battery", "battery"})
	if err != nil || got != bridge.TriggerBattery {
		t.Fatalf("got %v, %v", got, err)
	}
}

func TestParseTriggersRejectsUnknownNameNamingIt(t *testing.T) {
	_, err := bridge.ParseTriggers([]string{"battery", "doorbell"})
	if err == nil || !strings.Contains(err.Error(), `"doorbell"`) {
		t.Fatalf("err = %v, want one naming doorbell", err)
	}
}

func TestParseTriggersRejectsEmptyList(t *testing.T) {
	if _, err := bridge.ParseTriggers(nil); err == nil {
		t.Fatal("empty list accepted")
	}
}

func TestTriggerNamesAllIsCollapsed(t *testing.T) {
	if got := bridge.AllTriggers.Names(); !slices.Equal(got, []string{"all"}) {
		t.Fatalf("got %v", got)
	}
}

func TestTriggerNamesInBitOrderIgnoringHighBits(t *testing.T) {
	tr := bridge.TriggerOnlineStatus | bridge.TriggerStateChangedOpen | bridge.TriggerBattery | 1<<12
	want := []string{"state_changed_open", "battery", "online_status"}
	if got := tr.Names(); !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestTriggerNamesRoundTrip(t *testing.T) {
	cases := []bridge.Triggers{bridge.AllTriggers}
	for _, tc := range triggerNames {
		cases = append(cases, tc.bit)
	}
	for _, tr := range cases {
		got, err := bridge.ParseTriggers(tr.Names())
		if err != nil || got != tr {
			t.Errorf("round trip of %v = %v, %v", tr, got, err)
		}
	}
}

func TestListWebhooksDecodesTriggerFlags(t *testing.T) {
	body := `[
	 {"id":1,"url":"http://a/1","trigger_state_changed_open":1,"trigger_state_changed_latch":1,"trigger_state_changed_night_lock":1,
	  "trigger_state_changed_unknown":1,"trigger_state_goto_open":1,"trigger_state_goto_latch":1,
	  "trigger_state_goto_night_lock":1,"trigger_battery":1,"trigger_online_status":1},
	 {"id":"2","url":"http://a/2","trigger_battery":"1","trigger_online_status":"1","trigger_state_goto_open":"0","extra":"x"},
	 {"id":3,"url":"http://a/3"}]`
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(body)) }))
	hooks, err := c.ListWebhooks(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := []bridge.Triggers{bridge.AllTriggers, bridge.TriggerBattery | bridge.TriggerOnlineStatus, 0}
	if len(hooks) != len(want) {
		t.Fatalf("got %d hooks", len(hooks))
	}
	for i, h := range hooks {
		if h.Triggers != want[i] || int(h.ID) != i+1 {
			t.Errorf("hook %d = id %d triggers %v, want triggers %v", i, h.ID, h.Triggers, want[i])
		}
	}
}

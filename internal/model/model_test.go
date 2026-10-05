package model_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/internal/model"
)

func lockStr(l *model.LockState) string {
	if l == nil {
		return "<nil>"
	}
	return string(*l)
}

func TestFromStateReached(t *testing.T) {
	cases := []struct {
		eventType string
		lock      string
		bolt      loqed.BoltState
		event     model.EventType
	}{
		{"STATE_CHANGED_NIGHT_LOCK", "LOCKED", loqed.BoltNightLock, model.EventLocked},
		{"STATE_CHANGED_LATCH", "UNLOCKED", loqed.BoltDayLock, model.EventUnlocked},
		{"STATE_CHANGED_OPEN_REMOTE", "OPEN", loqed.BoltOpen, model.EventOpened},
		{"STATE_CHANGED_NIGHT_LOCK_REMOTE", "LOCKED", loqed.BoltNightLock, model.EventLocked},
		{"STATE_CHANGED_UNKNOWN", "<nil>", loqed.BoltUnknown, model.EventUnknown},
		{"SOMETHING_NEW", "<nil>", loqed.BoltUnknown, model.EventUnknown},
	}
	for _, c := range cases {
		tr := model.FromStateReached(c.eventType)
		if !tr.SetLock || lockStr(tr.Lock) != c.lock || !tr.SetBolt || tr.Bolt != c.bolt || tr.Event != c.event {
			t.Errorf("%s: %+v (lock %s)", c.eventType, tr, lockStr(tr.Lock))
		}
	}
	stall := model.FromStateReached("MOTOR_STALL")
	if !stall.SetLock || lockStr(stall.Lock) != "JAMMED" || stall.SetBolt || stall.Event != model.EventJammed {
		t.Errorf("motor stall: %+v", stall)
	}
}

func TestFromGoTo(t *testing.T) {
	locked := model.Locked
	tr := model.FromGoTo(loqed.BoltNightLock, nil)
	if !tr.SetLock || lockStr(tr.Lock) != "LOCKING" || tr.Event != model.EventLocking {
		t.Errorf("%+v", tr)
	}
	tr = model.FromGoTo(loqed.BoltNightLock, &locked)
	if tr.SetLock || tr.Event != model.EventLocking {
		t.Errorf("already locked must not flip to LOCKING: %+v", tr)
	}
	if tr := model.FromGoTo(loqed.BoltDayLock, &locked); lockStr(tr.Lock) != "UNLOCKING" || tr.Event != model.EventUnlocking {
		t.Errorf("%+v", tr)
	}
	if tr := model.FromGoTo(loqed.BoltOpen, &locked); lockStr(tr.Lock) != "OPENING" || tr.Event != model.EventOpening {
		t.Errorf("%+v", tr)
	}
	if tr := model.FromGoTo(loqed.BoltUnknown, &locked); tr.SetLock || tr.Event != model.EventUnknown {
		t.Errorf("%+v", tr)
	}
}

func TestApply(t *testing.T) {
	s := model.State{BoltState: loqed.BoltDayLock, Lock: model.LockStateFor(loqed.BoltDayLock)}
	s.Apply(model.FromGoTo(loqed.BoltNightLock, s.Lock))
	if lockStr(s.Lock) != "LOCKING" || s.BoltState != loqed.BoltDayLock {
		t.Fatalf("%+v", s)
	}
	s.Apply(model.FromStateReached("STATE_CHANGED_UNKNOWN"))
	if s.Lock != nil || s.BoltState != loqed.BoltUnknown {
		t.Fatalf("%+v", s)
	}
}

func TestSource(t *testing.T) {
	cases := map[string]string{
		"GO_TO_STATE_TWIST_ASSIST_LATCH":     "twist_assist",
		"GO_TO_STATE_INSTANTOPEN_OPEN":       "instant_open",
		"GO_TO_STATE_TOUCH_TO_LOCK":          "touch",
		"GO_TO_STATE_MANUAL_UNLOCK_BLE_OPEN": "manual",
		"STATE_CHANGED_LATCH_REMOTE":         "remote",
		"GO_TO_STATE_BLE_LATCH":              "ble",
		"STATE_CHANGED_NIGHT_LOCK":           "other",
		"go_to_state_touch_to_lock":          "touch",
	}
	for in, want := range cases {
		if got := model.Source(in); got != want {
			t.Errorf("%s: got %s want %s", in, got, want)
		}
	}
}

func TestNormalizeKeyID(t *testing.T) {
	if model.NormalizeKeyID(nil) != nil || model.NormalizeKeyID(model.Ptr(255)) != nil {
		t.Fatal("nil and 255 must normalize to nil")
	}
	if *model.NormalizeKeyID(model.Ptr(3)) != 3 {
		t.Fatal("3 stays 3")
	}
}

func TestParseCommand(t *testing.T) {
	for in, want := range map[string]model.Command{"LOCK": model.CommandLock, " unlock\n": model.CommandUnlock, "Open": model.CommandOpen} {
		got, ok := model.ParseCommand(in)
		if !ok || got != want {
			t.Errorf("%q: %v %v", in, got, ok)
		}
	}
	if _, ok := model.ParseCommand("RESET"); ok {
		t.Error("RESET must be rejected")
	}
	if model.CommandLock.Target() != loqed.BoltNightLock || model.CommandUnlock.Target() != loqed.BoltDayLock || model.CommandOpen.Target() != loqed.BoltOpen {
		t.Error("targets")
	}
	if model.CommandLock.Moving() != model.Locking || model.CommandOpen.Moving() != model.Opening {
		t.Error("moving states")
	}
}

func TestStateJSON(t *testing.T) {
	at := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	s := model.State{Lock: model.Ptr(model.Locked), BoltState: loqed.BoltNightLock, BatteryPercentage: model.Ptr(78),
		LockOnline: true, Mode: model.ModeLocal, LastEvent: "GO_TO_STATE_TOUCH_TO_LOCK", LastEventAt: &at}
	b, _ := json.Marshal(s)
	for _, want := range []string{`"lock":"LOCKED"`, `"bolt_state":"night_lock"`, `"battery_percentage":78`, `"mode":"local"`,
		`"last_key_id":null`, `"last_event_at":"2026-10-04T12:00:00Z"`, `"state_stale":false`} {
		if !strings.Contains(string(b), want) {
			t.Errorf("missing %s in %s", want, b)
		}
	}
	b, _ = json.Marshal(model.State{})
	if !strings.Contains(string(b), `"lock":null`) {
		t.Errorf("unknown lock must be null: %s", b)
	}
}

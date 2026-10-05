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

func TestFromStateReachedUnrecognized(t *testing.T) {
	tr := model.FromStateReached("SOMETHING_NEW")
	if tr.SetLock || tr.SetBolt || tr.Event != model.EventUnknown {
		t.Fatalf("%+v", tr)
	}
	s := model.State{BoltState: loqed.BoltNightLock, Lock: model.LockStateFor(loqed.BoltNightLock)}
	s.Apply(tr)
	if lockStr(s.Lock) != "LOCKED" || s.BoltState != loqed.BoltNightLock {
		t.Fatalf("unrecognized event wiped state: %+v", s)
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

func TestSourceFor(t *testing.T) {
	key := model.Ptr(3)
	parsed := map[string]string{
		"GO_TO_STATE_TWIST_ASSIST_LATCH":            "twist_assist",
		"GO_TO_STATE_INSTANTOPEN_OPEN":              "instant_open",
		"GO_TO_STATE_TOUCH_TO_LOCK":                 "touch",
		"go_to_state_touch_to_lock":                 "touch",
		"STATE_CHANGED_LATCH_REMOTE":                "remote",
		"GO_TO_STATE_MANUAL_LOCK_REMOTE_NIGHT_LOCK": "remote",
		"GO_TO_STATE_MANUAL_UNLOCK_BLE_OPEN":        "other",
		"STATE_CHANGED_NIGHT_LOCK":                  "other",
	}
	for in, want := range parsed {
		if got := model.SourceFor(in, key, false); got != want {
			t.Errorf("%s: got %s want %s", in, got, want)
		}
		if got := model.SourceFor(in, nil, false); got != model.SourceUnknown {
			t.Errorf("%s without a key: got %s want unknown", in, got)
		}
		if got := model.SourceFor(in, nil, true); got != model.SourceUnknown {
			t.Errorf("%s without a key is never the gateway: got %s", in, got)
		}
		if got := model.SourceFor(in, key, true); got != model.SourceGateway {
			t.Errorf("%s from the gateway: got %s", in, got)
		}
	}
}

func TestParseCommandMessage(t *testing.T) {
	ok := []struct {
		in  string
		cmd model.Command
		id  string
	}{
		{"LOCK", model.CommandLock, ""},
		{" unlock\n", model.CommandUnlock, ""},
		{"Open", model.CommandOpen, ""},
		{`{"command":"UNLOCK","id":"auto-42"}`, model.CommandUnlock, "auto-42"},
		{` {"command":"lock"} `, model.CommandLock, ""},
		{`{"command":"OPEN","id":null}`, model.CommandOpen, ""},
		{`{"command":"LOCK","id":"` + strings.Repeat("x", 64) + `"}`, model.CommandLock, strings.Repeat("x", 64)},
	}
	for _, c := range ok {
		cmd, id, err := model.ParseCommandMessage([]byte(c.in))
		if err != nil || cmd != c.cmd || id != c.id {
			t.Errorf("%q: got %q %q %v", c.in, cmd, id, err)
		}
	}
	bad := []string{
		"RESET", `{"command":"RESET"}`, `{"command":1}`, `{"command":"LOCK"`, `{"id":"x"}`, `[]`, "",
		`{"command":"LOCK","id":"` + strings.Repeat("x", 65) + `"}`,
		`{"command":"LOCK","id":"a\u0007b"}`,
		`{"command":"LOCK","id":7}`,
	}
	for _, in := range bad {
		_, _, err := model.ParseCommandMessage([]byte(in))
		if err == nil {
			t.Errorf("%q: expected error", in)
		} else if !strings.Contains(err.Error(), "LOCK, UNLOCK or OPEN") {
			t.Errorf("%q: error does not name the accepted forms: %v", in, err)
		}
	}
}

func TestCommandStatusJSONKeepsNullKeys(t *testing.T) {
	at := time.Date(2026, 10, 6, 12, 0, 0, 123456789, time.UTC)
	b, err := json.Marshal(model.CommandStatus{Command: model.CommandLock, Status: model.StatusPending, ReceivedAt: at, UpdatedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	want := `{"command":"LOCK","id":null,"status":"pending","via":null,"attempts":0,"error":null,` +
		`"received_at":"2026-10-06T12:00:00.123Z","updated_at":"2026-10-06T12:00:00.123Z"}`
	if string(b) != want {
		t.Fatalf("got  %s\nwant %s", b, want)
	}
	if len(model.CommandStatusValues) != 8 || model.CommandStatusValues[0] != model.StatusPending {
		t.Fatalf("statuses %v", model.CommandStatusValues)
	}
}

func TestStateTokenExpiryOmittedWhenUnknown(t *testing.T) {
	b, _ := json.Marshal(model.State{})
	if strings.Contains(string(b), "token_expires_at") {
		t.Fatalf("unknown expiry must be omitted: %s", b)
	}
	at := time.Date(2027, 4, 5, 19, 15, 26, 0, time.UTC)
	b, _ = json.Marshal(model.State{TokenExpiresAt: &at})
	if !strings.Contains(string(b), `"token_expires_at":"2027-04-05T19:15:26Z"`) {
		t.Fatalf("got %s", b)
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

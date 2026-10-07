package loqed_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	loqed "github.com/t3hk0d3/go-loqed"
)

func TestParseBoltState(t *testing.T) {
	cases := map[string]loqed.BoltState{
		"NIGHT_LOCK": loqed.BoltNightLock, "night_lock": loqed.BoltNightLock,
		"DAY_LOCK": loqed.BoltDayLock, "day_lock": loqed.BoltDayLock, "LATCH": loqed.BoltDayLock, "latch": loqed.BoltDayLock,
		"OPEN": loqed.BoltOpen, " open ": loqed.BoltOpen,
		"UNKNOWN": loqed.BoltUnknown, "": loqed.BoltUnknown, "weird": loqed.BoltUnknown,
	}
	for in, want := range cases {
		if got := loqed.ParseBoltState(in); got != want {
			t.Errorf("%q: got %q want %q", in, got, want)
		}
	}
}

func TestReachedState(t *testing.T) {
	cases := []struct {
		in     string
		state  loqed.BoltState
		jammed bool
	}{
		{"STATE_CHANGED_OPEN", loqed.BoltOpen, false},
		{"STATE_CHANGED_OPEN_REMOTE", loqed.BoltOpen, false},
		{"STATE_CHANGED_LATCH", loqed.BoltDayLock, false},
		{"STATE_CHANGED_LATCH_REMOTE", loqed.BoltDayLock, false},
		{"STATE_CHANGED_NIGHT_LOCK", loqed.BoltNightLock, false},
		{"STATE_CHANGED_NIGHT_LOCK_REMOTE", loqed.BoltNightLock, false},
		{"STATE_CHANGED_UNKNOWN", loqed.BoltUnknown, false},
		{"MOTOR_STALL", loqed.BoltUnknown, true},
		{"SOMETHING_NEW", loqed.BoltUnknown, false},
	}
	for _, c := range cases {
		s, j := loqed.ReachedState(c.in)
		if s != c.state || j != c.jammed {
			t.Errorf("%s: got %q,%v want %q,%v", c.in, s, j, c.state, c.jammed)
		}
	}
}

func TestGoToTarget(t *testing.T) {
	cases := []struct {
		et, goTo string
		want     loqed.BoltState
	}{
		{"GO_TO_STATE_TWIST_ASSIST_LATCH", "DAY_LOCK", loqed.BoltDayLock},
		{"GO_TO_STATE_INSTANTOPEN_OPEN", "", loqed.BoltOpen},
		{"GO_TO_STATE_TOUCH_TO_LOCK", "", loqed.BoltNightLock},
		{"GO_TO_STATE_MANUAL_UNLOCK_VIA_OUTSIDE_LATCH", "", loqed.BoltDayLock},
		{"GO_TO_STATE_MANUAL_UNLOCK_VIA_OUTSIDE_MODULE_PIN", "", loqed.BoltOpen},
		{"GO_TO_STATE_WHATEVER", "", loqed.BoltUnknown},
	}
	for _, c := range cases {
		if got := loqed.GoToTarget(c.et, c.goTo); got != c.want {
			t.Errorf("%s/%s: got %q want %q", c.et, c.goTo, got, c.want)
		}
	}
	if !loqed.IsGoToState("go_to_state_touch_to_lock") || loqed.IsGoToState("STATE_CHANGED_OPEN") {
		t.Error("IsGoToState")
	}
}

func TestBoltStateUnmarshalNormalizes(t *testing.T) {
	var v struct {
		S loqed.BoltState `json:"s"`
	}
	if err := json.Unmarshal([]byte(`{"s":"NIGHT_LOCK"}`), &v); err != nil {
		t.Fatal(err)
	}
	if v.S != loqed.BoltNightLock {
		t.Fatalf("got %q", v.S)
	}
}

func TestIsServerError(t *testing.T) {
	if !loqed.IsServerError(fmt.Errorf("wrap: %w", &loqed.APIError{StatusCode: 502})) {
		t.Error("502 should be a server error")
	}
	if loqed.IsServerError(&loqed.APIError{StatusCode: 404}) {
		t.Error("404 is not a server error")
	}
	if loqed.IsServerError(errors.New("x")) {
		t.Error("plain error is not a server error")
	}
}

package bridge_test

import (
	"encoding/base64"
	"errors"
	"strings"
	"testing"
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/bridge"
)

const goldenEventBody = `{"requested_state":"NIGHT_LOCK","requested_state_numeric":3,"mac_wifi":"aa","mac_ble":"bb","event_type":"STATE_CHANGED_NIGHT_LOCK","key_local_id":255}`
const goldenEventHash = "a1caf7ab481834cdcb2f42ffd36e81599091485f39b76f6a88212b443107a4c7"

func key(t *testing.T) []byte {
	t.Helper()
	k, err := base64.StdEncoding.DecodeString(testBridgeKey)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func TestParseEventGoldenStateReached(t *testing.T) {
	ev, err := bridge.ParseEvent(key(t), []byte(goldenEventBody), goldenEventHash, "1700000000", fixedNow)
	if err != nil {
		t.Fatal(err)
	}
	sr, ok := ev.(bridge.StateReachedEvent)
	if !ok {
		t.Fatalf("got %T", ev)
	}
	if sr.EventType != "STATE_CHANGED_NIGHT_LOCK" || sr.BoltState != loqed.BoltNightLock || sr.Jammed ||
		sr.KeyLocalID != nil || sr.MacWifi != "aa" || sr.MacBLE != "bb" {
		t.Fatalf("got %+v", sr)
	}
}

func TestParseEventAcceptsUpperCaseHash(t *testing.T) {
	if _, err := bridge.ParseEvent(key(t), []byte(goldenEventBody), strings.ToUpper(goldenEventHash), "1700000000", fixedNow); err != nil {
		t.Fatal(err)
	}
}

func TestParseEventSignatureErrors(t *testing.T) {
	k := key(t)
	body := []byte(goldenEventBody)
	cases := []struct {
		name, hash, ts string
		now            time.Time
		want           error
	}{
		{"missing hash", "", "1700000000", fixedNow, loqed.ErrBadSignature},
		{"missing timestamp", goldenEventHash, "", fixedNow, loqed.ErrBadSignature},
		{"malformed timestamp", goldenEventHash, "abc", fixedNow, loqed.ErrBadSignature},
		{"wrong hash", strings.Repeat("0", 64), "1700000000", fixedNow, loqed.ErrBadSignature},
		{"wrong hash and stale", strings.Repeat("0", 64), "1700000000", fixedNow.Add(time.Hour), loqed.ErrBadSignature},
		{"negative timestamp", goldenEventHash, "-5", fixedNow, loqed.ErrBadSignature},
		{"too old", goldenEventHash, "1700000000", fixedNow.Add(21 * time.Second), loqed.ErrStaleTimestamp},
		{"too new", goldenEventHash, "1700000000", fixedNow.Add(-21 * time.Second), loqed.ErrStaleTimestamp},
	}
	for _, c := range cases {
		_, err := bridge.ParseEvent(k, body, c.hash, c.ts, c.now)
		if !errors.Is(err, c.want) {
			t.Errorf("%s: got %v want %v", c.name, err, c.want)
		}
	}
	// Whole seconds are compared, like loqedAPI: 20.9 s is still accepted.
	if _, err := bridge.ParseEvent(k, body, goldenEventHash, "1700000000", fixedNow.Add(20900*time.Millisecond)); err != nil {
		t.Errorf("20.9s skew should pass: %v", err)
	}
}

func TestParseEventAcceptsLateDeliveryWithinDefaultTolerance(t *testing.T) {
	// A bridge with many registered webhooks delivers to them one after
	// another, so a webhook can arrive well after its TIMESTAMP.
	if _, err := bridge.ParseEvent(key(t), []byte(goldenEventBody), goldenEventHash, "1700000000", fixedNow.Add(13*time.Second)); err != nil {
		t.Fatalf("a webhook delivered 13s late should pass: %v", err)
	}
}

func TestParseEventWithinUsesTheGivenTolerance(t *testing.T) {
	k, body := key(t), []byte(goldenEventBody)
	if _, err := bridge.ParseEventWithin(k, body, goldenEventHash, "1700000000", fixedNow.Add(59*time.Second), time.Minute); err != nil {
		t.Errorf("59s within a 1m tolerance should pass: %v", err)
	}
	for _, d := range []time.Duration{61 * time.Second, -61 * time.Second} {
		if _, err := bridge.ParseEventWithin(k, body, goldenEventHash, "1700000000", fixedNow.Add(d), time.Minute); !errors.Is(err, loqed.ErrStaleTimestamp) {
			t.Errorf("%v outside a 1m tolerance: got %v", d, err)
		}
	}
}

func TestParseEventWithinZeroToleranceAcceptsAnyAge(t *testing.T) {
	k, body := key(t), []byte(goldenEventBody)
	if _, err := bridge.ParseEventWithin(k, body, goldenEventHash, "1700000000", fixedNow.Add(24*time.Hour), 0); err != nil {
		t.Errorf("tolerance 0 should skip the timestamp check: %v", err)
	}
	if _, err := bridge.ParseEventWithin(k, body, strings.Repeat("0", 64), "1700000000", fixedNow, 0); !errors.Is(err, loqed.ErrBadSignature) {
		t.Errorf("tolerance 0 must still check HASH: got %v", err)
	}
}

func TestParseEventStaleMentionsSkew(t *testing.T) {
	_, err := bridge.ParseEvent(key(t), []byte(goldenEventBody), goldenEventHash, "1700000000", fixedNow.Add(42*time.Second))
	if err == nil || !strings.Contains(err.Error(), "42s") {
		t.Fatalf("got %v", err)
	}
}

// sign produces a valid signature for arbitrary bodies at fixedNow.
func sign(t *testing.T, body string) (string, string) {
	t.Helper()
	// Recompute exactly like the bridge: sha256(body | ts8 | key).
	h := hashForTest(append(append([]byte(body), be64ForTest(1700000000)...), key(t)...))
	return h, "1700000000"
}

func TestParseEventFamilies(t *testing.T) {
	cases := []struct {
		name  string
		body  string
		check func(t *testing.T, ev bridge.Event)
	}{
		{"go to state", `{"go_to_state":"DAY_LOCK","go_to_state_numeric":2,"mac_wifi":"a","mac_ble":"b","event_type":"GO_TO_STATE_TWIST_ASSIST_LATCH","key_local_id":"3"}`,
			func(t *testing.T, ev bridge.Event) {
				g := ev.(bridge.GoToStateEvent)
				if g.GoToState != loqed.BoltDayLock || g.EventType != "GO_TO_STATE_TWIST_ASSIST_LATCH" || *g.KeyLocalID != 3 {
					t.Fatalf("%+v", g)
				}
			}},
		{"state reached null key", `{"requested_state":"OPEN","event_type":"STATE_CHANGED_OPEN_REMOTE","key_local_id":null}`,
			func(t *testing.T, ev bridge.Event) {
				s := ev.(bridge.StateReachedEvent)
				if s.BoltState != loqed.BoltOpen || s.KeyLocalID != nil {
					t.Fatalf("%+v", s)
				}
			}},
		// HA fixture nightlock_reached.json: requested NIGHT_LOCK but the bolt latched.
		{"reached state comes from event_type", `{"requested_state":"NIGHT_LOCK","event_type":"STATE_CHANGED_LATCH","key_local_id":1}`,
			func(t *testing.T, ev bridge.Event) {
				s := ev.(bridge.StateReachedEvent)
				if s.BoltState != loqed.BoltDayLock || s.RequestedState != loqed.BoltNightLock {
					t.Fatalf("%+v", s)
				}
			}},
		{"motor stall never reports the requested state", `{"requested_state":"NIGHT_LOCK","event_type":"MOTOR_STALL","key_local_id":2}`,
			func(t *testing.T, ev bridge.Event) {
				s := ev.(bridge.StateReachedEvent)
				if s.EventType != "MOTOR_STALL" || !s.Jammed || s.BoltState != loqed.BoltUnknown || *s.KeyLocalID != 2 {
					t.Fatalf("%+v", s)
				}
			}},
		{"go to state without go_to_state field", `{"event_type":"GO_TO_STATE_TOUCH_TO_LOCK","key_local_id":255}`,
			func(t *testing.T, ev bridge.Event) {
				g := ev.(bridge.GoToStateEvent)
				if g.GoToState != loqed.BoltNightLock || g.KeyLocalID != nil {
					t.Fatalf("%+v", g)
				}
			}},
		{"battery", `{"battery_type":"1","battery_percentage":"88","mac_wifi":"a","mac_ble":"b"}`,
			func(t *testing.T, ev bridge.Event) {
				b := ev.(bridge.BatteryEvent)
				if b.BatteryPercentage != 88 || b.BatteryType != "1" || b.BLEStrength != nil {
					t.Fatalf("%+v", b)
				}
			}},
		{"online", `{"wifi_strength":"-60","ble_strength":-1,"mac_wifi":"a","mac_ble":"b"}`,
			func(t *testing.T, ev bridge.Event) {
				o := ev.(bridge.OnlineEvent)
				if *o.WifiStrength != -60 || *o.BLEStrength != -1 {
					t.Fatalf("%+v", o)
				}
			}},
		{"empty key means no key", `{"requested_state":"DAY_LOCK","event_type":"STATE_CHANGED_LATCH","key_local_id":""}`,
			func(t *testing.T, ev bridge.Event) {
				if ev.(bridge.StateReachedEvent).KeyLocalID != nil {
					t.Fatal("expected nil key")
				}
			}},
		{"absent key means no key", `{"event_type":"GO_TO_STATE_MANUAL_LOCK_REMOTE_LATCH"}`,
			func(t *testing.T, ev bridge.Event) {
				if ev.(bridge.GoToStateEvent).KeyLocalID != nil {
					t.Fatal("expected nil key")
				}
			}},
		{"key 0 is a real key", `{"event_type":"STATE_CHANGED_LATCH","key_local_id":"0"}`,
			func(t *testing.T, ev bridge.Event) {
				if k := ev.(bridge.StateReachedEvent).KeyLocalID; k == nil || *k != 0 {
					t.Fatalf("got %v", k)
				}
			}},
		{"out of range key", `{"requested_state":"OPEN","event_type":"STATE_CHANGED_OPEN","key_local_id":999}`,
			func(t *testing.T, ev bridge.Event) {
				if ev.(bridge.StateReachedEvent).KeyLocalID != nil {
					t.Fatal("expected nil key")
				}
			}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h, ts := sign(t, c.body)
			ev, err := bridge.ParseEvent(key(t), []byte(c.body), h, ts, fixedNow)
			if err != nil {
				t.Fatal(err)
			}
			c.check(t, ev)
		})
	}
}

func TestParseEventInvalidPayload(t *testing.T) {
	for _, body := range []string{`not json`, `[]`, `{"mac_wifi":"a"}`} {
		h, ts := sign(t, body)
		if _, err := bridge.ParseEvent(key(t), []byte(body), h, ts, fixedNow); !errors.Is(err, loqed.ErrInvalidPayload) {
			t.Errorf("%s: got %v", body, err)
		}
	}
}

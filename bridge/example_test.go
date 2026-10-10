package bridge_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/bridge"
)

// fakeBridge stands in for a LOQED Bridge in the examples. It answers
// /status and /to_lock, and keeps webhook registrations in memory.
type fakeBridge struct {
	mu     sync.Mutex
	hooks  []map[string]any
	nextID int
}

func (f *fakeBridge) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/status":
		_, _ = io.WriteString(w, `{"battery_percentage":88,"bolt_state":"night_lock","lock_online":1,"wifi_strength":45}`)
	case r.Method == http.MethodGet && r.URL.Path == "/to_lock":
		// A real bridge answers every command with 200, even one the lock
		// will reject.
	case r.Method == http.MethodGet && r.URL.Path == "/webhooks":
		_ = json.NewEncoder(w).Encode(f.hooks)
	case r.Method == http.MethodPost && r.URL.Path == "/webhooks":
		var h map[string]any
		_ = json.NewDecoder(r.Body).Decode(&h)
		f.nextID++
		h["id"] = f.nextID
		f.hooks = append(f.hooks, h)
	case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/webhooks/"):
		id, _ := strconv.Atoi(strings.TrimPrefix(r.URL.Path, "/webhooks/"))
		kept := f.hooks[:0]
		for _, h := range f.hooks {
			if h["id"] != id {
				kept = append(kept, h)
			}
		}
		f.hooks = kept
	default:
		http.NotFound(w, r)
	}
}

// startFakeBridge returns the IP:port of a fake bridge and a stop function.
func startFakeBridge() (addr string, stop func()) {
	srv := httptest.NewServer(&fakeBridge{})
	return srv.Listener.Addr().String(), srv.Close
}

// In real use the address and credentials come from cloud.Client.ListLocks:
// Lock.BridgeIP, Lock.BridgeKey, Lock.KeySecret and Lock.LocalID.
func ExampleNew() {
	addr, stop := startFakeBridge() // a real bridge: "192.168.1.20"
	defer stop()

	c, err := bridge.New(addr, bridge.Credentials{
		BridgeKey:  "Ym9uam91ciBtb25kZQ==",
		KeySecret:  "SGFsbG8gd2VyZWxk",
		LocalKeyID: 1,
	})
	if err != nil {
		log.Fatal(err)
	}

	st, err := c.Status(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("bolt:", st.BoltState, "battery:", st.BatteryPercentage)

	// Hostnames are rejected: a bridge is addressed by IP only.
	_, err = bridge.New("loqed-bridge.local", bridge.Credentials{})
	fmt.Println(err != nil)
	// Output:
	// bolt: night_lock battery: 88
	// true
}

func ExampleClient_Command() {
	addr, stop := startFakeBridge()
	defer stop()
	c, err := bridge.New(addr, bridge.Credentials{
		BridgeKey: "Ym9uam91ciBtb25kZQ==", KeySecret: "SGFsbG8gd2VyZWxk", LocalKeyID: 1,
	})
	if err != nil {
		log.Fatal(err)
	}

	err = c.Command(context.Background(), bridge.ActionLock)
	switch {
	case err == nil:
		// The bridge received the command. Only the webhooks that follow
		// (GO_TO_STATE_* with key 1, then STATE_CHANGED_*) confirm it.
		fmt.Println("sent; waiting for the lock's webhooks")
	case errors.Is(err, loqed.ErrUnreachable):
		fmt.Println("never left: safe to send again")
	case errors.Is(err, loqed.ErrNoResponse):
		fmt.Println("may have arrived: do not send again")
	default:
		fmt.Println("failed:", err)
	}
	// Output:
	// sent; waiting for the lock's webhooks
}

func ExampleClient_Status() {
	addr, stop := startFakeBridge()
	defer stop()
	// Status needs no credentials.
	c, err := bridge.New(addr, bridge.Credentials{})
	if err != nil {
		log.Fatal(err)
	}
	st, err := c.Status(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	// /status lags behind the lock: treat it as a hint, not as confirmation.
	fmt.Println(st.BoltState, st.LockOnline == 1, st.WifiStrength)
	// Output:
	// night_lock true 45
}

func ExampleClient_CreateWebhook() {
	addr, stop := startFakeBridge()
	defer stop()
	c, err := bridge.New(addr, bridge.Credentials{BridgeKey: "Ym9uam91ciBtb25kZQ=="})
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()

	triggers, err := bridge.ParseTriggers([]string{"state_changed_open", "state_changed_latch", "state_changed_night_lock"})
	if err != nil {
		log.Fatal(err)
	}
	if err := c.CreateWebhook(ctx, "http://192.168.1.10:8099/webhook/front-door", bridge.AllTriggers); err != nil {
		log.Fatal(err)
	}
	if err := c.CreateWebhook(ctx, "http://192.168.1.11:8123/api/webhook/abc", triggers); err != nil {
		log.Fatal(err)
	}

	hooks, err := c.ListWebhooks(ctx)
	if err != nil {
		log.Fatal(err)
	}
	for _, h := range hooks {
		fmt.Println(h.ID, h.URL, h.Triggers.Names())
	}

	// Every registration delays the others: remove the ones you no longer need.
	if err := c.DeleteWebhook(ctx, int(hooks[1].ID)); err != nil {
		log.Fatal(err)
	}
	hooks, err = c.ListWebhooks(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(len(hooks), "webhook left")
	// Output:
	// 1 http://192.168.1.10:8099/webhook/front-door [all]
	// 2 http://192.168.1.11:8123/api/webhook/abc [state_changed_open state_changed_latch state_changed_night_lock]
	// 1 webhook left
}

// A handler for the webhooks the bridge sends: verify, then decode.
func ExampleParseEvent() {
	bridgeKey, _ := base64.StdEncoding.DecodeString("Ym9uam91ciBtb25kZQ==") // or bridge.Client.BridgeKey()

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(io.LimitReader(r.Body, 64<<10))
		if err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		ev, err := bridge.ParseEvent(bridgeKey, body, r.Header.Get("HASH"), r.Header.Get("TIMESTAMP"), time.Now())
		switch {
		case errors.Is(err, loqed.ErrBadSignature), errors.Is(err, loqed.ErrStaleTimestamp):
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		case err != nil:
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		switch ev := ev.(type) {
		case bridge.GoToStateEvent:
			fmt.Println("moving to", ev.GoToState, "key", *ev.KeyLocalID)
		case bridge.StateReachedEvent:
			fmt.Println("reached", ev.BoltState, "jammed", ev.Jammed)
		case bridge.BatteryEvent:
			fmt.Println("battery", ev.BatteryPercentage)
		case bridge.OnlineEvent:
			fmt.Println("signal report")
		}
	})

	// What the bridge sends: HASH = hex(sha256(body | TIMESTAMP as 8-byte
	// big-endian | bridge key)), like testdata/gen_vectors.py.
	send := func(body string, ts time.Time) int {
		sum := sha256.Sum256(append(binary.BigEndian.AppendUint64([]byte(body), uint64(ts.Unix())), bridgeKey...))
		req := httptest.NewRequest(http.MethodPost, "/webhook/front-door", bytes.NewReader([]byte(body)))
		req.Header.Set("TIMESTAMP", strconv.FormatInt(ts.Unix(), 10))
		req.Header.Set("HASH", hex.EncodeToString(sum[:]))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec.Code
	}

	now := time.Now()
	send(`{"event_type":"GO_TO_STATE_MANUAL_UNLOCK_VIA_APP","go_to_state":"DAY_LOCK","key_local_id":1}`, now)
	send(`{"requested_state":"DAY_LOCK","event_type":"STATE_CHANGED_LATCH","key_local_id":1}`, now)
	fmt.Println("replayed:", send(`{"event_type":"STATE_CHANGED_OPEN"}`, now.Add(-time.Hour)))
	// Output:
	// moving to day_lock key 1
	// reached day_lock jammed false
	// replayed: 403
}

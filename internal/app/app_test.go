package app_test

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/t3hk0d3/go-loqed/internal/app"
	"github.com/t3hk0d3/go-loqed/internal/config"
	"github.com/t3hk0d3/go-loqed/internal/model"
	"github.com/t3hk0d3/go-loqed/internal/store"
	"github.com/t3hk0d3/go-loqed/internal/testutil"
	"github.com/t3hk0d3/go-loqed/internal/webhook"
)

const bridgeKey = "Ym9uam91ciBtb25kZQ==" // "bonjour monde"

type fakeBridge struct {
	mu       sync.Mutex
	webhooks []string
	actions  []byte
	bolt     string
}

func (f *fakeBridge) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch {
	case r.URL.Path == "/status":
		_, _ = fmt.Fprintf(w, `{"bolt_state":%q,"lock_online":1,"battery_percentage":80,"wifi_strength":70,"ble_strength":40}`, f.bolt)
	case r.URL.Path == "/webhooks" && r.Method == http.MethodGet:
		list := []map[string]any{}
		for i, u := range f.webhooks {
			list = append(list, map[string]any{"id": i + 1, "url": u})
		}
		_ = json.NewEncoder(w).Encode(list)
	case r.URL.Path == "/webhooks" && r.Method == http.MethodPost:
		var body struct{ URL string }
		_ = json.NewDecoder(r.Body).Decode(&body)
		f.webhooks = append(f.webhooks, body.URL)
	case r.URL.Path == "/to_lock":
		raw, _ := url.QueryUnescape(strings.TrimPrefix(r.URL.RawQuery, "command_signed_base64="))
		cmd, _ := base64.StdEncoding.DecodeString(raw)
		if len(cmd) > 0 {
			f.actions = append(f.actions, cmd[len(cmd)-1])
		}
	default:
		http.NotFound(w, r)
	}
}

func (f *fakeBridge) snapshot() ([]string, []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.webhooks...), append([]byte(nil), f.actions...)
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// fakeCloud serves GET /api/locks/ for one lock whose bridge is bridgeHost.
func fakeCloud(t *testing.T, bridgeHost string, calls *atomic.Int32) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Header.Get("Authorization") != "Bearer tok" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_, _ = fmt.Fprintf(w, `{"data":[{"id":"lock1","name":"Front door","model_name":"LOQED Touch","bolt_state":"day_lock","online":true,
			"bridge_ip":%q,"local_id":1,"key_secret":"SGFsbG8gd2VyZWxk","bridge_key":%q,"bridge_mac_wifi":"aa:bb:cc:dd:ee:ff"}]}`, bridgeHost, bridgeKey)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func baseConfig(cachePath, broker string) config.Config {
	cfg := config.Defaults()
	cfg.CloudToken = "tok"
	cfg.CachePath = cachePath
	cfg.Webhook.Listen = "127.0.0.1:0"
	cfg.MQTT.URL = broker
	return cfg
}

// start runs the app and returns its webhook address and a stop function
// that cancels it and waits for a clean return.
func start(t *testing.T, cfg config.Config, cloudURL string) (string, func()) {
	t.Helper()
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan string, 1)
	done := make(chan error, 1)
	go func() {
		done <- app.Run(ctx, app.Options{Config: cfg, Log: slog.New(slog.DiscardHandler), Version: "e2e",
			CloudBaseURL: cloudURL, Ready: func(addr string) { ready <- addr }})
	}()
	var addr string
	select {
	case addr = <-ready:
	case err := <-done:
		t.Fatalf("app exited: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("app did not start")
	}
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("app returned %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("app did not stop")
		}
	}
	t.Cleanup(stop)
	return addr, stop
}

func TestEndToEnd(t *testing.T) {
	broker := testutil.StartBroker(t)
	sub := testutil.Subscribe(t, broker, "#")

	fb := &fakeBridge{bolt: "day_lock"}
	bridgeSrv := httptest.NewServer(fb)
	defer bridgeSrv.Close()
	var cloudCalls atomic.Int32
	cloudSrv := fakeCloud(t, strings.TrimPrefix(bridgeSrv.URL, "http://"), &cloudCalls)

	// A stale cache built with another token, listing a lock that no longer exists.
	cachePath := filepath.Join(t.TempDir(), "locks.json")
	_ = os.WriteFile(cachePath, []byte(`{"version":1,"token_sha256":"old","published_ids":["gone"],"locks":[{"id":"gone","name":"Old door"}]}`), 0o600)

	cfg := baseConfig(cachePath, broker)
	cfg.MQTT.ClientID = "gw-e2e"
	cfg.Webhook.PublicURL = "https://loqed.example.com"
	cfg.Webhook.CloudSecret = "0123456789abcdef0123456789abcdef"
	addr, stop := start(t, cfg, cloudSrv.URL)

	sub.WaitFor(t, 10*time.Second, func(m testutil.Message) bool {
		return m.Topic == "homeassistant/device/loqed_lock1/config" && len(m.Payload) > 0
	})
	for _, topic := range []string{"homeassistant/device/loqed_gone/config", "loqed/gone/state"} {
		sub.WaitFor(t, 10*time.Second, func(m testutil.Message) bool { return m.Topic == topic && len(m.Payload) == 0 })
	}
	sub.WaitFor(t, 10*time.Second, func(m testutil.Message) bool {
		var s model.State
		return m.Topic == "loqed/lock1/state" && json.Unmarshal(m.Payload, &s) == nil && s.Lock != nil && *s.Lock == model.Unlocked && s.Mode == model.ModeLocal
	})
	if n := cloudCalls.Load(); n != 1 {
		t.Fatalf("expected exactly one cloud call, got %d", n)
	}

	resp, err := http.Get("http://" + addr + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	var health webhook.HealthReport
	_ = json.NewDecoder(resp.Body).Decode(&health)
	_ = resp.Body.Close()
	if resp.StatusCode != 200 || health.Locks["lock1"].Mode != model.ModeLocal {
		t.Fatalf("healthz %d %+v", resp.StatusCode, health)
	}

	var hookURL string
	eventually(t, "webhook registration", func() bool {
		hooks, _ := fb.snapshot()
		if len(hooks) == 1 {
			hookURL = hooks[0]
		}
		return hookURL != ""
	})

	sub.Publish(t, "loqed/lock1/command", "LOCK", false)
	eventually(t, "bridge command", func() bool {
		_, actions := fb.snapshot()
		return len(actions) == 1 && actions[0] == 3
	})

	body := `{"requested_state":"NIGHT_LOCK","event_type":"STATE_CHANGED_NIGHT_LOCK","key_local_id":255,"mac_wifi":"aa","mac_ble":"bb"}`
	ts := time.Now().Unix()
	h := sha256.New()
	h.Write([]byte(body))
	h.Write(binary.BigEndian.AppendUint64(nil, uint64(ts)))
	h.Write([]byte("bonjour monde"))
	req, _ := http.NewRequest(http.MethodPost, hookURL, strings.NewReader(body))
	req.Header["TIMESTAMP"] = []string{strconv.FormatInt(ts, 10)} // verbatim, like the bridge
	req.Header["HASH"] = []string{hex.EncodeToString(h.Sum(nil))}
	resp, err = http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("webhook post: %v %v", resp, err)
	}
	_ = resp.Body.Close()

	sub.WaitFor(t, 10*time.Second, func(m testutil.Message) bool {
		var s model.State
		return m.Topic == "loqed/lock1/state" && json.Unmarshal(m.Payload, &s) == nil && s.Lock != nil && *s.Lock == model.Locked
	})
	sub.WaitFor(t, 10*time.Second, func(m testutil.Message) bool {
		var e model.Event
		return m.Topic == "loqed/lock1/event" && json.Unmarshal(m.Payload, &e) == nil && e.EventType == model.EventLocked
	})

	// Cloud route: enriches the bridge event with the key name.
	cloudBody := `{"requested_state":"NIGHT_LOCK","event_type":"STATE_CHANGED_NIGHT_LOCK","lock_id":"lock1","key_local_id":255,"key_name_user":"Hallway phone"}`
	resp, err = http.Post("http://"+addr+"/cloud/"+cfg.Webhook.CloudSecret, "application/json", strings.NewReader(cloudBody))
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("cloud webhook: %v %v", resp, err)
	}
	_ = resp.Body.Close()

	stop()
	sub.WaitFor(t, 10*time.Second, func(m testutil.Message) bool {
		return m.Topic == "loqed/status" && string(m.Payload) == "offline"
	})
	snap, _, _ := store.Open(cachePath)
	if c := snap.Snapshot(); len(c.Budget.Calls) != 1 || len(c.PublishedIDs) != 1 || c.PublishedIDs[0] != "lock1" || c.InstallID == "" {
		t.Fatalf("cache after run: budget %v published %v install %q", c.Budget.Calls, c.PublishedIDs, c.InstallID)
	}
}

// A typo in the allow-list must not spend a cloud call on every restart.
func TestRestartWithTypoInAllowListUsesCache(t *testing.T) {
	broker := testutil.StartBroker(t)
	fb := &fakeBridge{bolt: "day_lock"}
	bridgeSrv := httptest.NewServer(fb)
	defer bridgeSrv.Close()
	var calls atomic.Int32
	cloudSrv := fakeCloud(t, strings.TrimPrefix(bridgeSrv.URL, "http://"), &calls)
	cachePath := filepath.Join(t.TempDir(), "locks.json")

	cfg := baseConfig(cachePath, broker)
	cfg.Locks = []string{"Front door", "Frnt door"}
	for i := range 3 {
		cfg.MQTT.ClientID = "gw-restart-" + strconv.Itoa(i)
		_, stop := start(t, cfg, cloudSrv.URL)
		stop()
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("restarts called the cloud %d times", n)
	}
}

func TestFailsWithoutCacheOrCloud(t *testing.T) {
	cfg := baseConfig(filepath.Join(t.TempDir(), "locks.json"), "tcp://127.0.0.1:1")
	err := app.Run(context.Background(), app.Options{Config: cfg, Log: slog.New(slog.DiscardHandler), CloudBaseURL: "http://127.0.0.1:1"})
	if err == nil || !strings.Contains(err.Error(), "no credential cache") {
		t.Fatalf("got %v", err)
	}
}

func TestFailsWithoutAnyCredentials(t *testing.T) {
	cfg := config.Defaults()
	cfg.CachePath = filepath.Join(t.TempDir(), "locks.json")
	err := app.Run(context.Background(), app.Options{Config: cfg, Log: slog.New(slog.DiscardHandler)})
	if err == nil || !strings.Contains(err.Error(), "cloud_token") {
		t.Fatalf("got %v", err)
	}
}

func TestCancelDuringStartupIsCleanExit(t *testing.T) {
	cfg := baseConfig(filepath.Join(t.TempDir(), "locks.json"), "tcp://127.0.0.1:1")
	hang := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer hang.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := app.Run(ctx, app.Options{Config: cfg, Log: slog.New(slog.DiscardHandler), CloudBaseURL: hang.URL}); err != nil {
		t.Fatalf("a stop during startup must be a clean exit: %v", err)
	}
}

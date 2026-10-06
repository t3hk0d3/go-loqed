package app_test

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/t3hk0d3/go-loqed/internal/app"
	"github.com/t3hk0d3/go-loqed/internal/config"
	"github.com/t3hk0d3/go-loqed/internal/gateway"
	"github.com/t3hk0d3/go-loqed/internal/model"
	"github.com/t3hk0d3/go-loqed/internal/store"
	"github.com/t3hk0d3/go-loqed/internal/testutil"
	"github.com/t3hk0d3/go-loqed/internal/webhook"
)

const bridgeKey = "Ym9uam91ciBtb25kZQ==" // "bonjour monde"

// fakeBridge behaves like the real bridge (spec 2.1): it answers every
// /to_lock with 200, acts only on a valid, fresh signature, announces the
// movement with GO_TO_STATE_* and STATE_CHANGED_* webhooks, and updates
// /status only after the webhooks were delivered.
type fakeBridge struct {
	mu       sync.Mutex
	webhooks []string
	actions  []byte // executed (validly signed) actions
	requests int    // /to_lock calls, valid or not
	bolt     string
	secret   []byte // key secret the lock accepts; nil = the fake cloud's
}

const keySecret = "SGFsbG8gd2VyZWxk" // "Hallo werld", served by fakeCloud

var moves = map[byte]struct{ goTo, goToState, changed, bolt string }{
	1: {"GO_TO_STATE_INSTANTOPEN_OPEN", "OPEN", "STATE_CHANGED_OPEN", "open"},
	2: {"GO_TO_STATE_MANUAL_UNLOCK_REMOTE_LATCH", "DAY_LOCK", "STATE_CHANGED_LATCH", "day_lock"},
	3: {"GO_TO_STATE_MANUAL_LOCK_REMOTE_NIGHT_LOCK", "NIGHT_LOCK", "STATE_CHANGED_NIGHT_LOCK", "night_lock"},
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
		f.requests++
		raw, _ := url.QueryUnescape(strings.TrimPrefix(r.URL.RawQuery, "command_signed_base64="))
		cmd, _ := base64.StdEncoding.DecodeString(raw)
		if key, action, ok := f.verify(cmd); ok {
			f.actions = append(f.actions, action)
			go f.move(key, action, append([]string(nil), f.webhooks...))
		}
		_, _ = w.Write([]byte("Message resent to the lock"))
	default:
		http.NotFound(w, r)
	}
}

// verify checks the HMAC and the timestamp like the lock does.
func (f *fakeBridge) verify(cmd []byte) (key, action byte, ok bool) {
	if len(cmd) != 8+2+8+sha256.Size+3 {
		return 0, 0, false
	}
	secret := f.secret
	if secret == nil {
		secret, _ = base64.StdEncoding.DecodeString(keySecret)
	}
	ts := int64(binary.BigEndian.Uint64(cmd[10:18]))
	key, action = cmd[len(cmd)-3], cmd[len(cmd)-1]
	signed := append(append([]byte{}, cmd[8:18]...), key, cmd[len(cmd)-2], action)
	mac := hmac.New(sha256.New, secret)
	mac.Write(signed)
	fresh := time.Since(time.Unix(ts, 0)) < 60*time.Second
	return key, action, fresh && hmac.Equal(mac.Sum(nil), cmd[18:18+sha256.Size])
}

func (f *fakeBridge) move(key, action byte, hooks []string) {
	m := moves[action]
	time.Sleep(50 * time.Millisecond)
	for _, h := range hooks {
		postSigned(h, fmt.Sprintf(`{"go_to_state":%q,"event_type":%q,"key_local_id":%d,"mac_wifi":"aa","mac_ble":"bb"}`, m.goToState, m.goTo, key))
	}
	time.Sleep(150 * time.Millisecond)
	for _, h := range hooks {
		postSigned(h, fmt.Sprintf(`{"requested_state":%q,"event_type":%q,"key_local_id":%d,"mac_wifi":"aa","mac_ble":"bb"}`, m.goToState, m.changed, key))
	}
	f.mu.Lock()
	f.bolt = m.bolt // /status catches up after the webhooks
	f.mu.Unlock()
}

// postSigned POSTs a bridge webhook signed with the bridge key.
func postSigned(hookURL, body string) {
	ts := time.Now().Unix()
	h := sha256.New()
	h.Write([]byte(body))
	h.Write(binary.BigEndian.AppendUint64(nil, uint64(ts)))
	h.Write([]byte("bonjour monde"))
	req, _ := http.NewRequest(http.MethodPost, hookURL, strings.NewReader(body))
	req.Header["TIMESTAMP"] = []string{strconv.FormatInt(ts, 10)} // verbatim, like the bridge
	req.Header["HASH"] = []string{hex.EncodeToString(h.Sum(nil))}
	if resp, err := http.DefaultClient.Do(req); err == nil {
		_ = resp.Body.Close()
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
			"bridge_ip":%q,"local_id":1,"key_secret":%q,"bridge_key":%q,"bridge_mac_wifi":"aa:bb:cc:dd:ee:ff"}]}`, bridgeHost, keySecret, bridgeKey)
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
func start(t *testing.T, cfg config.Config, cloudURL string, timing ...func(*gateway.Timing)) (string, func()) {
	t.Helper()
	return startWith(t, cfg, cloudURL, func(o *app.Options) {
		if len(timing) > 0 {
			o.Timing = timing[0]
		}
	})
}

// startWith is start with full control over the options (log, timing).
func startWith(t *testing.T, cfg config.Config, cloudURL string, tweak func(*app.Options)) (string, func()) {
	t.Helper()
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan string, 1)
	done := make(chan error, 1)
	go func() {
		o := app.Options{Config: cfg, Log: slog.New(slog.DiscardHandler), Version: "e2e",
			CloudBaseURL: cloudURL, Ready: func(addr string) { ready <- addr }}
		tweak(&o)
		done <- app.Run(ctx, o)
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

	eventually(t, "webhook registration", func() bool {
		hooks, _ := fb.snapshot()
		return len(hooks) == 1
	})

	sub.Publish(t, "loqed/lock1/command", "LOCK", false)
	eventually(t, "bridge command", func() bool {
		_, actions := fb.snapshot()
		return len(actions) == 1 && actions[0] == 3
	})
	// The fake bridge announces the movement through its webhooks.
	sub.WaitFor(t, 10*time.Second, func(m testutil.Message) bool {
		var s model.State
		return m.Topic == "loqed/lock1/state" && json.Unmarshal(m.Payload, &s) == nil && s.Lock != nil && *s.Lock == model.Locked
	})
	sub.WaitFor(t, 10*time.Second, func(m testutil.Message) bool {
		var e model.Event
		return m.Topic == "loqed/lock1/event" && json.Unmarshal(m.Payload, &e) == nil && e.EventType == model.EventLocked &&
			e.Source != nil && *e.Source == model.SourceGateway
	})

	// Cloud route: enriches the bridge event with the key name.
	cloudBody := `{"requested_state":"NIGHT_LOCK","event_type":"STATE_CHANGED_NIGHT_LOCK","lock_id":6148,"key_local_id":"","key_name_user":"Hallway phone"}`
	resp, err = http.Post("http://"+addr+"/cloud/"+cfg.Webhook.CloudSecret+"/lock1", "application/json", strings.NewReader(cloudBody))
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
	if rec, _ := snap.Snapshot().Find("lock1"); rec.CloudWebhookID != "6148" {
		t.Fatalf("the cloud lock id must be learned and persisted: %+v", rec)
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

// A cache holding no locks must not stick: the next start asks the cloud.
func TestEmptyLockCacheIsRefreshedOnNextStart(t *testing.T) {
	broker := testutil.StartBroker(t)
	fb := &fakeBridge{bolt: "day_lock"}
	bridgeSrv := httptest.NewServer(fb)
	defer bridgeSrv.Close()
	var calls atomic.Int32
	full := fakeCloud(t, strings.TrimPrefix(bridgeSrv.URL, "http://"), &calls)
	var empty atomic.Bool
	empty.Store(true)
	cloudSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if empty.Load() {
			_, _ = w.Write([]byte(`{"data":[]}`))
			return
		}
		full.Config.Handler.ServeHTTP(w, r)
	}))
	defer cloudSrv.Close()
	cfg := baseConfig(filepath.Join(t.TempDir(), "locks.json"), broker)
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	err := app.Run(context.Background(), app.Options{Config: cfg, Log: slog.New(slog.DiscardHandler), CloudBaseURL: cloudSrv.URL})
	if err == nil || !strings.Contains(err.Error(), "no locks to manage") {
		t.Fatalf("got %v", err)
	}
	empty.Store(false)
	start(t, cfg, cloudSrv.URL) // fails the test if the app exits instead of starting
}

// A removed lock stays in published_ids until its retained topics have
// really been cleared (the broker may be down when the lock disappears).
func TestRemovedIDsStayPublishedUntilCleared(t *testing.T) {
	broker := testutil.StartBroker(t)
	fb := &fakeBridge{bolt: "day_lock"}
	bridgeSrv := httptest.NewServer(fb)
	defer bridgeSrv.Close()
	var calls atomic.Int32
	cloudSrv := fakeCloud(t, strings.TrimPrefix(bridgeSrv.URL, "http://"), &calls)
	cachePath := filepath.Join(t.TempDir(), "locks.json")
	cfg := baseConfig(cachePath, broker)
	_, stop := start(t, cfg, cloudSrv.URL)
	stop()

	st, _, _ := store.Open(cachePath)
	if err := st.Update(func(c *store.Cache) { c.PublishedIDs = []string{"lock1", "ghost"} }); err != nil {
		t.Fatal(err)
	}
	published := func() []string {
		s, _, _ := store.Open(cachePath)
		return s.Snapshot().PublishedIDs
	}

	down := cfg
	down.MQTT.URL = "tcp://127.0.0.1:1" // broker unreachable: nothing can be cleared
	_, stop = start(t, down, cloudSrv.URL)
	stop()
	if got := published(); len(got) != 2 {
		t.Fatalf("ghost dropped from published_ids before it was cleared: %v", got)
	}

	sub := testutil.Subscribe(t, broker, "#")
	cfg.MQTT.ClientID = "gw-cleared"
	start(t, cfg, cloudSrv.URL)
	sub.WaitFor(t, 10*time.Second, func(m testutil.Message) bool {
		return m.Topic == "loqed/ghost/state" && len(m.Payload) == 0
	})
	eventually(t, "published_ids without the cleared lock", func() bool { return len(published()) == 1 })
}

// commandStatuses collects the command_status trail of lock1.
func commandStatuses(sub *testutil.Subscriber) []model.CommandStatus {
	var out []model.CommandStatus
	for _, m := range sub.Messages() {
		var st model.CommandStatus
		if m.Topic == "loqed/lock1/command_status" && json.Unmarshal(m.Payload, &st) == nil {
			out = append(out, st)
		}
	}
	return out
}

func startLock(t *testing.T, fb *fakeBridge, timing ...func(*gateway.Timing)) *testutil.Subscriber {
	t.Helper()
	broker := testutil.StartBroker(t)
	sub := testutil.Subscribe(t, broker, "loqed/#")
	bridgeSrv := httptest.NewServer(fb)
	t.Cleanup(bridgeSrv.Close)
	var calls atomic.Int32
	cloudSrv := fakeCloud(t, strings.TrimPrefix(bridgeSrv.URL, "http://"), &calls)
	cfg := baseConfig(filepath.Join(t.TempDir(), "locks.json"), broker)
	cfg.MQTT.ClientID = "gw-" + t.Name()
	start(t, cfg, cloudSrv.URL, timing...)
	eventually(t, "webhook registration", func() bool {
		hooks, _ := fb.snapshot()
		return len(hooks) == 1
	})
	return sub
}

func TestEndToEndCommandIsConfirmedByWebhooks(t *testing.T) {
	fb := &fakeBridge{bolt: "night_lock"}
	sub := startLock(t, fb)
	sub.Publish(t, "loqed/lock1/command", "UNLOCK", false)
	sub.WaitFor(t, 10*time.Second, func(m testutil.Message) bool {
		var st model.CommandStatus
		return m.Topic == "loqed/lock1/command_status" && json.Unmarshal(m.Payload, &st) == nil && st.Status == model.StatusConfirmed
	})
	var trail []model.CommandStatusValue
	for _, st := range commandStatuses(sub) {
		trail = append(trail, st.Status)
	}
	want := []model.CommandStatusValue{model.StatusPending, model.StatusSending, model.StatusSent, model.StatusAccepted, model.StatusConfirmed}
	if !slices.Equal(trail, want) {
		t.Fatalf("command_status %v, want %v", trail, want)
	}
	var locks []model.LockState
	for _, m := range sub.Messages() {
		var s model.State
		if m.Topic == "loqed/lock1/state" && json.Unmarshal(m.Payload, &s) == nil && s.Lock != nil {
			locks = append(locks, *s.Lock)
		}
	}
	if !slices.Contains(locks, model.Unlocking) || locks[len(locks)-1] != model.Unlocked {
		t.Fatalf("lock states %v", locks)
	}
	fb.mu.Lock()
	defer fb.mu.Unlock()
	if fb.requests != 1 {
		t.Fatalf("exactly one /to_lock call, got %d", fb.requests)
	}
}

// The bridge acknowledges commands it cannot verify; only the missing
// webhooks show that nothing happened.
func TestEndToEndUnverifiedCommandFailsWithoutConfirmation(t *testing.T) {
	fb := &fakeBridge{bolt: "day_lock", secret: []byte("another key")}
	sub := startLock(t, fb, func(tm *gateway.Timing) { tm.WebhookConfirm = time.Second })
	sub.Publish(t, "loqed/lock1/command", `{"command":"LOCK","id":"e2e"}`, false)
	sub.WaitFor(t, 10*time.Second, func(m testutil.Message) bool {
		var st model.CommandStatus
		return m.Topic == "loqed/lock1/command_status" && json.Unmarshal(m.Payload, &st) == nil && st.Status == model.StatusFailed
	})
	sts := commandStatuses(sub)
	last := sts[len(sts)-1]
	if last.Error == nil || *last.Error != model.FailNoConfirmation || last.ID == nil || *last.ID != "e2e" {
		t.Fatalf("status %+v", last)
	}
	_, actions := fb.snapshot()
	if len(actions) != 0 {
		t.Fatalf("the lock must not act on a bad signature: %v", actions)
	}
}

// countingServer answers every request with 500 and counts them.
func countingServer(t *testing.T, calls *atomic.Int32) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// readOnlyDir makes dir unwritable for the rest of the test.
func readOnlyDir(t *testing.T, dir string) {
	t.Helper()
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
}

func TestUnwritableCacheFailsBeforeAnyCloudCall(t *testing.T) {
	var cloudCalls, portalCalls atomic.Int32
	cloudSrv, portalSrv := countingServer(t, &cloudCalls), countingServer(t, &portalCalls)
	dir := t.TempDir()
	path := filepath.Join(dir, "locks.json")
	cfg := baseConfig(path, "tcp://127.0.0.1:1")
	cfg.CloudToken, cfg.CloudEmail, cfg.CloudPassword = "", "user@example.com", "pw" // a mint would be attempted
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	readOnlyDir(t, dir)
	err := app.Run(context.Background(), app.Options{Config: cfg, Log: slog.New(slog.DiscardHandler),
		CloudBaseURL: cloudSrv.URL, PortalBaseURL: portalSrv.URL})
	if !errors.Is(err, store.ErrWrite) || !strings.Contains(err.Error(), path) {
		t.Fatalf("got %v", err)
	}
	if c, p := cloudCalls.Load(), portalCalls.Load(); c != 0 || p != 0 {
		t.Fatalf("cloud calls %d, portal calls %d before failing", c, p)
	}
}

func TestUnwritableExistingCacheFailsBeforeAnyCloudCall(t *testing.T) {
	var cloudCalls atomic.Int32
	cloudSrv := countingServer(t, &cloudCalls)
	dir := t.TempDir()
	path := filepath.Join(dir, "locks.json")
	st, _, _ := store.Open(path)
	if _, err := st.InstallID(); err != nil {
		t.Fatal(err)
	}
	cfg := baseConfig(path, "tcp://127.0.0.1:1")
	readOnlyDir(t, dir)
	err := app.Run(context.Background(), app.Options{Config: cfg, Log: slog.New(slog.DiscardHandler), CloudBaseURL: cloudSrv.URL})
	if !errors.Is(err, store.ErrWrite) || !strings.Contains(err.Error(), path) {
		t.Fatalf("got %v", err)
	}
	if c := cloudCalls.Load(); c != 0 {
		t.Fatalf("%d cloud calls before failing", c)
	}
}

// The writability check must not mistake an unreadable cache for an
// unwritable one: a corrupt file is rebuilt from the cloud.
func TestCorruptCacheInWritableDirectoryStillStarts(t *testing.T) {
	broker := testutil.StartBroker(t)
	bridgeSrv := httptest.NewServer(&fakeBridge{bolt: "day_lock"})
	defer bridgeSrv.Close()
	var calls atomic.Int32
	cloudSrv := fakeCloud(t, strings.TrimPrefix(bridgeSrv.URL, "http://"), &calls)
	path := filepath.Join(t.TempDir(), "locks.json")
	if err := os.WriteFile(path, []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	start(t, baseConfig(path, broker), cloudSrv.URL)
	if st, status, _ := store.Open(path); status != store.StatusLoaded || len(st.Snapshot().Locks) != 1 {
		t.Fatalf("cache not rebuilt: status %v", status)
	}
}

func TestCloudSecretIsSavedBeforeFirstCloudCall(t *testing.T) {
	broker := testutil.StartBroker(t)
	bridgeSrv := httptest.NewServer(&fakeBridge{bolt: "day_lock"})
	defer bridgeSrv.Close()
	var calls atomic.Int32
	full := fakeCloud(t, strings.TrimPrefix(bridgeSrv.URL, "http://"), &calls)
	path := filepath.Join(t.TempDir(), "locks.json")
	var first sync.Once
	var secretFirst atomic.Bool
	cloudSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		first.Do(func() {
			st, _, _ := store.Open(path)
			secretFirst.Store(st.Snapshot().CloudSecret != "")
		})
		full.Config.Handler.ServeHTTP(w, r)
	}))
	defer cloudSrv.Close()
	cfg := baseConfig(path, broker)
	cfg.Webhook.PublicURL = "https://gw.example.com"
	start(t, cfg, cloudSrv.URL)
	if calls.Load() == 0 {
		t.Fatal("no cloud call made")
	}
	if !secretFirst.Load() {
		t.Fatal("the cloud webhook secret was not saved before the first cloud call")
	}
}

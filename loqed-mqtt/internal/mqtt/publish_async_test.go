package mqtt_test

import (
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/loqed-mqtt/internal/model"
	"github.com/t3hk0d3/go-loqed/loqed-mqtt/internal/mqtt"
)

// fakeToken is a paho token the test completes (or never completes).
type fakeToken struct {
	done chan struct{}
	once sync.Once
	err  error
}

func newFakeToken() *fakeToken { return &fakeToken{done: make(chan struct{})} }

func (t *fakeToken) complete(err error) {
	t.once.Do(func() { t.err = err; close(t.done) })
}

func (t *fakeToken) Wait() bool { <-t.done; return true }
func (t *fakeToken) WaitTimeout(d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-t.done:
		return true
	case <-timer.C:
		return false
	}
}
func (t *fakeToken) Done() <-chan struct{} { return t.done }
func (t *fakeToken) Error() error {
	select {
	case <-t.done:
		return t.err
	default:
		return nil
	}
}

type fakePub struct {
	topic   string
	payload string
	tok     *fakeToken
}

// fakePaho is a paho client without a network. Publishes to topics for which
// withhold returns true get a token that only the test completes; all others
// are acknowledged at once.
type fakePaho struct {
	mu       sync.Mutex
	open     bool
	withhold func(topic string) bool
	pubs     []fakePub
}

func (f *fakePaho) done() *fakeToken { t := newFakeToken(); t.complete(nil); return t }

func (f *fakePaho) IsConnected() bool      { return f.IsConnectionOpen() }
func (f *fakePaho) IsConnectionOpen() bool { f.mu.Lock(); defer f.mu.Unlock(); return f.open }
func (f *fakePaho) Connect() paho.Token    { return f.done() }
func (f *fakePaho) Disconnect(uint)        {}
func (f *fakePaho) Publish(topic string, _ byte, _ bool, payload any) paho.Token {
	var p string
	switch v := payload.(type) {
	case []byte:
		p = string(v)
	case string:
		p = v
	}
	tok := newFakeToken()
	if f.withhold == nil || !f.withhold(topic) {
		tok.complete(nil)
	}
	f.mu.Lock()
	f.pubs = append(f.pubs, fakePub{topic: topic, payload: p, tok: tok})
	f.mu.Unlock()
	return tok
}
func (f *fakePaho) Subscribe(string, byte, paho.MessageHandler) paho.Token { return f.done() }
func (f *fakePaho) SubscribeMultiple(map[string]byte, paho.MessageHandler) paho.Token {
	return f.done()
}
func (f *fakePaho) Unsubscribe(...string) paho.Token     { return f.done() }
func (f *fakePaho) AddRoute(string, paho.MessageHandler) {}
func (f *fakePaho) OptionsReader() paho.ClientOptionsReader {
	return paho.NewOptionsReader(paho.NewClientOptions())
}

func (f *fakePaho) published(topic string) []fakePub {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []fakePub
	for _, p := range f.pubs {
		if p.topic == topic {
			out = append(out, p)
		}
	}
	return out
}

func withholdAll(string) bool { return true }

// newFakeClient returns a client on a fake paho client and its log.
func newFakeClient(t *testing.T, fp *fakePaho, cfg mqtt.ClientConfig) (*mqtt.Client, *logBuffer) {
	t.Helper()
	logs := &logBuffer{}
	cfg.URL, cfg.ClientID, cfg.Topics = "tcp://127.0.0.1:1", "gw-"+t.Name(), topics
	c := mqtt.NewClient(cfg, slog.New(slog.NewTextHandler(logs, nil)))
	c.SetPaho(fp)
	return c, logs
}

func TestClientSetsWriteTimeout(t *testing.T) {
	c := mqtt.NewClient(mqtt.ClientConfig{URL: "tcp://127.0.0.1:1", ClientID: "x", Topics: topics}, slog.New(slog.DiscardHandler))
	r := c.PahoOptions()
	if got := r.WriteTimeout(); got != 2*time.Second {
		t.Fatalf("WriteTimeout = %v, want 2s", got)
	}
}

func TestLockPublishDoesNotWaitForPubAck(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fp := &fakePaho{open: true, withhold: withholdAll}
		c, _ := newFakeClient(t, fp, mqtt.ClientConfig{})
		defer c.Close()
		start := time.Now()
		if err := c.PublishState("lock1", model.State{BoltState: loqed.BoltNightLock}); err != nil {
			t.Fatalf("PublishState: %v", err)
		}
		if err := c.PublishCommandStatus("lock1", model.CommandStatus{Command: model.CommandOpen, Status: model.StatusSending}); err != nil {
			t.Fatalf("PublishCommandStatus: %v", err)
		}
		if err := c.PublishEvent("lock1", model.Event{EventType: model.EventCommandFailed}); err != nil {
			t.Fatalf("PublishEvent: %v", err)
		}
		if waited := time.Since(start); waited != 0 {
			t.Fatalf("lock publishes waited %v for the broker", waited)
		}
		if n := len(fp.published("loqed/lock1/state")); n != 1 {
			t.Fatalf("state handed to paho %d times, want 1", n)
		}
	})
}

func TestMissingPubAckIsLoggedOnceAfter30s(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fp := &fakePaho{open: true, withhold: withholdAll}
		c, logs := newFakeClient(t, fp, mqtt.ClientConfig{})
		defer c.Close()
		_ = c.PublishState("lock1", model.State{BoltState: loqed.BoltNightLock})
		_ = c.PublishState("lock1", model.State{BoltState: loqed.BoltOpen})
		time.Sleep(29 * time.Second)
		synctest.Wait()
		if strings.Contains(logs.String(), "publishing state failed") {
			t.Fatalf("warned before 30 s:\n%s", logs)
		}
		time.Sleep(2 * time.Second)
		synctest.Wait()
		out := logs.String()
		if n := strings.Count(out, "publishing state failed"); n != 1 {
			t.Fatalf("got %d warnings, want 1 (rate-limited):\n%s", n, out)
		}
		if strings.Contains(out, "night_lock") || strings.Contains(out, "loqed/lock1") {
			t.Fatalf("warning leaks the payload or topic:\n%s", out)
		}
	})
}

func TestFailedPublishIsLoggedWithoutPayload(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fp := &fakePaho{open: true, withhold: withholdAll}
		c, logs := newFakeClient(t, fp, mqtt.ClientConfig{})
		defer c.Close()
		_ = c.PublishEvent("lock1", model.Event{EventType: model.EventCommandFailed, Reason: "OPEN"})
		fp.published("loqed/lock1/event")[0].tok.complete(errors.New("connection lost"))
		synctest.Wait()
		out := logs.String()
		if !strings.Contains(out, "publishing event failed") || !strings.Contains(out, "connection lost") {
			t.Fatalf("no failure warning:\n%s", out)
		}
		if strings.Contains(out, "OPEN") {
			t.Fatalf("warning leaks the payload:\n%s", out)
		}
	})
}

func TestAcknowledgedPublishLogsNothing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fp := &fakePaho{open: true, withhold: withholdAll}
		c, logs := newFakeClient(t, fp, mqtt.ClientConfig{})
		defer c.Close()
		_ = c.PublishState("lock1", model.State{BoltState: loqed.BoltNightLock})
		fp.published("loqed/lock1/state")[0].tok.complete(nil)
		time.Sleep(time.Minute)
		synctest.Wait()
		if strings.Contains(logs.String(), "failed") {
			t.Fatalf("unexpected warning:\n%s", logs)
		}
	})
}

func TestCloseEndsOutcomeWatchers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fp := &fakePaho{open: true, withhold: withholdAll}
		c, logs := newFakeClient(t, fp, mqtt.ClientConfig{})
		_ = c.PublishState("lock1", model.State{BoltState: loqed.BoltNightLock})
		fp.mu.Lock()
		fp.open = false // Close publishes "offline" only while connected
		fp.mu.Unlock()
		c.Close()
		synctest.Wait() // the bubble fails if a watcher goroutine is left behind
		if strings.Contains(logs.String(), "failed") {
			t.Fatalf("warned after Close:\n%s", logs)
		}
	})
}

func TestNothingIsHandedToPahoWhileDisconnected(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fp := &fakePaho{open: false}
		c, _ := newFakeClient(t, fp, mqtt.ClientConfig{})
		defer c.Close()
		if err := c.PublishState("lock1", model.State{BoltState: loqed.BoltNightLock}); err != nil {
			t.Fatalf("PublishState while disconnected: %v", err)
		}
		_ = c.PublishEvent("lock1", model.Event{EventType: model.EventCommandFailed})
		if n := len(fp.published("loqed/lock1/state")) + len(fp.published("loqed/lock1/event")); n != 0 {
			t.Fatalf("%d publishes handed to paho while disconnected", n)
		}
		fp.mu.Lock()
		fp.open = true
		fp.mu.Unlock()
		c.Reconnected()
		got := fp.published("loqed/lock1/state")
		if len(got) != 1 || !strings.Contains(got[0].payload, "night_lock") {
			t.Fatalf("reconnect republished %v, want the cached state", got)
		}
		if n := len(fp.published("loqed/lock1/event")); n != 0 {
			t.Fatalf("event replayed %d times after reconnect", n)
		}
	})
}

func TestRemovedLockCleanupStillWaitsForAcknowledgement(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fp := &fakePaho{open: true, withhold: func(topic string) bool { return strings.HasPrefix(topic, "loqed/gone/") }}
		var cleared []string
		c, _ := newFakeClient(t, fp, mqtt.ClientConfig{OnRemovedCleared: func(ids []string) { cleared = append(cleared, ids...) }})
		defer c.Close()
		c.SetLocks(nil, []string{"gone"})
		c.Reconnected()
		if len(cleared) != 0 {
			t.Fatalf("removed lock reported cleared without acknowledgement: %v", cleared)
		}
	})
}

func TestRepublishNeverSendsAnOlderValueAfterANewerOne(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fp := &fakePaho{open: false}
		c, _ := newFakeClient(t, fp, mqtt.ClientConfig{})
		defer c.Close()
		_ = c.PublishState("lock1", model.State{BoltState: loqed.BoltNightLock}) // cached while disconnected
		fp.mu.Lock()
		fp.open = true
		fp.mu.Unlock()
		go c.Reconnected()
		go func() { _ = c.PublishState("lock1", model.State{BoltState: loqed.BoltOpen}) }()
		synctest.Wait()
		got := fp.published("loqed/lock1/state")
		if len(got) == 0 || !strings.Contains(got[len(got)-1].payload, `"open"`) {
			t.Fatalf("last state published = %v, want the newest (open)", got)
		}
	})
}

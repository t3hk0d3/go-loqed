package gateway

import (
	"context"
	"log/slog"
	"slices"
	"strconv"
	"testing"
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/bridge"
	"github.com/t3hk0d3/go-loqed/cloud"
	"github.com/t3hk0d3/go-loqed/loqed-mqtt/internal/config"
	"github.com/t3hk0d3/go-loqed/loqed-mqtt/internal/model"
	"github.com/t3hk0d3/go-loqed/loqed-mqtt/internal/store"
)

type fakeBridge struct {
	status      bridge.Status
	statusErr   error
	statusCalls int
	commandErrs []error // consumed per call; empty = success
	commands    []bridge.Action
	onCommand   func()                          // optional hook run inside Command
	hang        func(ctx context.Context) error // optional: runs inside Command, its error is returned
	cmdBudgets  []time.Duration                 // time left on each Command context
	hooks       []bridge.Webhook
	listErr     error
	listCalls   int
	created     []string
	deleted     []int
	createErrs  []error  // consumed per call; empty = success
	deleteErrs  []error  // consumed per call; empty = success
	calls       []string // "list", "create <url>", "delete <id>", "command" in order
	nextID      int      // last id handed out by CreateWebhook (0: start at 100)
	// webhooksNumber overrides /status webhooks_number (nil: len(hooks)).
	webhooksNumber *int
}

func (f *fakeBridge) Status(context.Context) (*bridge.Status, error) {
	f.statusCalls++
	if f.statusErr != nil {
		return nil, f.statusErr
	}
	st := f.status
	st.WebhooksNumber = loqed.Int(len(f.hooks))
	if f.webhooksNumber != nil {
		st.WebhooksNumber = loqed.Int(*f.webhooksNumber)
	}
	return &st, nil
}

func (f *fakeBridge) Command(ctx context.Context, a bridge.Action) error {
	f.commands = append(f.commands, a)
	f.calls = append(f.calls, "command")
	if dl, ok := ctx.Deadline(); ok {
		f.cmdBudgets = append(f.cmdBudgets, time.Until(dl))
	}
	if f.onCommand != nil {
		f.onCommand()
	}
	if f.hang != nil {
		return f.hang(ctx)
	}
	if len(f.commandErrs) > 0 {
		err := f.commandErrs[0]
		f.commandErrs = f.commandErrs[1:]
		return err
	}
	return nil
}

func (f *fakeBridge) ListWebhooks(context.Context) ([]bridge.Webhook, error) {
	f.listCalls++
	f.calls = append(f.calls, "list")
	if f.listErr != nil {
		return nil, f.listErr
	}
	return slices.Clone(f.hooks), nil
}

func (f *fakeBridge) CreateWebhook(_ context.Context, url string, t bridge.Triggers) error {
	f.calls = append(f.calls, "create "+url)
	if len(f.createErrs) > 0 {
		err := f.createErrs[0]
		f.createErrs = f.createErrs[1:]
		if err != nil {
			return err
		}
	}
	f.created = append(f.created, url)
	if f.nextID == 0 {
		f.nextID = 99
	}
	f.nextID++
	f.hooks = append(f.hooks, bridge.Webhook{ID: loqed.Int(f.nextID), URL: url, Triggers: t})
	return nil
}

func (f *fakeBridge) DeleteWebhook(_ context.Context, id int) error {
	f.calls = append(f.calls, "delete "+strconv.Itoa(id))
	if len(f.deleteErrs) > 0 {
		err := f.deleteErrs[0]
		f.deleteErrs = f.deleteErrs[1:]
		if err != nil {
			return err
		}
	}
	f.deleted = append(f.deleted, id)
	f.hooks = slices.DeleteFunc(f.hooks, func(h bridge.Webhook) bool { return int(h.ID) == id })
	return nil
}

type fakeCloud struct {
	now        func() time.Time
	locks      []cloud.Lock
	fetchedAt  time.Time // zero = now
	err        error
	calls      []Priority
	notBefore  []time.Time
	commands   []loqed.BoltState
	commandErr error
	cmdBudgets []time.Duration // time left on the command context
}

func (f *fakeCloud) Locks(_ context.Context, p Priority, notBefore time.Time) (LockList, error) {
	f.calls = append(f.calls, p)
	f.notBefore = append(f.notBefore, notBefore)
	if f.err != nil {
		return LockList{}, f.err
	}
	at := f.fetchedAt
	if at.IsZero() {
		at = f.now()
	}
	return LockList{Locks: f.locks, FetchedAt: at}, nil
}

func (f *fakeCloud) Command(ctx context.Context, _ string, s loqed.BoltState) error {
	dl, _ := ctx.Deadline()
	f.cmdBudgets = append(f.cmdBudgets, time.Until(dl))
	f.commands = append(f.commands, s)
	return f.commandErr
}

type fakePub struct {
	states   []model.State
	events   []model.Event
	avail    []bool
	statuses []model.CommandStatus
	lists    []model.WebhookList
	results  []model.WebhooksResult
}

func (f *fakePub) PublishWebhooks(_ string, l model.WebhookList) error {
	f.lists = append(f.lists, l)
	return nil
}

func (f *fakePub) PublishWebhooksResult(_ string, r model.WebhooksResult) error {
	f.results = append(f.results, r)
	return nil
}

func (f *fakePub) PublishCommandStatus(_ string, s model.CommandStatus) error {
	f.statuses = append(f.statuses, s)
	return nil
}

func (f *fakePub) PublishState(_ string, s model.State) error {
	f.states = append(f.states, s)
	return nil
}
func (f *fakePub) PublishEvent(_ string, e model.Event) error {
	f.events = append(f.events, e)
	return nil
}
func (f *fakePub) PublishAvailability(_ string, on bool) error {
	f.avail = append(f.avail, on)
	return nil
}

var t0 = time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)

func testRecord() store.LockRecord {
	id := 1
	return store.LockRecord{ID: "lock1", Name: "Front door", BridgeIP: "192.0.2.10", LocalID: &id,
		KeySecret: "SGFsbG8gd2VyZWxk", BridgeKey: "Ym9uam91ciBtb25kZQ=="}
}

type harness struct {
	t             *testing.T
	now           time.Time
	bridge        *fakeBridge
	cloud         *fakeCloud
	pub           *fakePub
	probeErr      error
	probes        []string
	cloudProbeErr error
	cloudProbes   int
	refreshRec    *store.LockRecord
	refreshErr    error
	refreshes     []Reason
	newBridgeErr  error
	bridgesBuilt  int
	s             *Supervisor
}

func newHarness(t *testing.T, rec store.LockRecord, setting config.LockSetting, tweak ...func(*Deps)) *harness {
	t.Helper()
	h := &harness{
		t: t, now: t0,
		bridge: &fakeBridge{status: bridge.Status{BoltState: loqed.BoltDayLock, LockOnline: 1, BatteryPercentage: 80, BatteryVoltage: 10.4}},
		pub:    &fakePub{},
	}
	h.cloud = &fakeCloud{now: func() time.Time { return h.now },
		locks: []cloud.Lock{{ID: "lock1", BoltState: loqed.BoltNightLock, BatteryPercentage: 70, Online: model.Ptr(true)}}}
	d := Deps{
		Publisher: h.pub,
		Cloud:     h.cloud,
		Refresh: func(_ context.Context, _ string, r Reason) (store.LockRecord, error) {
			h.refreshes = append(h.refreshes, r)
			if h.refreshErr != nil {
				return store.LockRecord{}, h.refreshErr
			}
			if h.refreshRec != nil {
				return *h.refreshRec, nil
			}
			return rec, nil
		},
		NewBridge: func(store.LockRecord) (BridgeAPI, error) {
			if h.newBridgeErr != nil {
				return nil, h.newBridgeErr
			}
			h.bridgesBuilt++
			return h.bridge, nil
		},
		Probe: func(_ context.Context, addr string) error {
			h.probes = append(h.probes, addr)
			return h.probeErr
		},
		ProbeCloud: func(context.Context) error {
			h.cloudProbes++
			return h.cloudProbeErr
		},
		WebhookURL: func(r store.LockRecord) (string, error) { return "http://10.0.0.5:8099/webhook/" + r.ID, nil },
		Now:        func() time.Time { return h.now },
		Log:        slog.New(slog.DiscardHandler),
	}
	for _, f := range tweak {
		f(&d)
	}
	h.s = NewSupervisor(rec, setting, d, DefaultTiming(60*time.Second, 24*time.Hour, 72*time.Minute))
	return h
}

func (h *harness) start()                  { h.s.start(context.Background()) }
func (h *harness) advance(d time.Duration) { h.now = h.now.Add(d); h.s.tick(context.Background()) }
func (h *harness) send(msg any)            { h.s.handle(context.Background(), msg) }

// command sends a command that arrived over MQTT just now.
func (h *harness) command(c model.Command) { h.send(CommandMsg{Command: c, At: h.now}) }

// run advances the clock in 1 s ticks, like Run's ticker.
func (h *harness) run(d time.Duration) {
	for end := h.now.Add(d); h.now.Before(end); {
		h.advance(time.Second)
	}
}

func (h *harness) state() model.State {
	h.t.Helper()
	if len(h.pub.states) == 0 {
		h.t.Fatal("no state published")
	}
	return h.pub.states[len(h.pub.states)-1]
}

func (h *harness) lock() string {
	if l := h.state().Lock; l != nil {
		return string(*l)
	}
	return "<nil>"
}

func (h *harness) available() bool { return h.pub.avail[len(h.pub.avail)-1] }

// src renders an event source ("" for null).
func src(e model.Event) string {
	if e.Source == nil {
		return ""
	}
	return *e.Source
}

// failedCommands returns the error classes of command_failed events.
func (h *harness) failedCommands() []string {
	var out []string
	for _, e := range h.pub.events {
		if e.EventType == model.EventCommandFailed {
			out = append(out, e.Error)
		}
	}
	return out
}

// toCloud drives a started local harness into cloud mode via 3 probe failures.
func (h *harness) toCloud() {
	h.t.Helper()
	h.probeErr = loqed.ErrUnreachable
	for range 3 {
		h.advance(60 * time.Second)
	}
	if h.s.mode != model.ModeCloud {
		h.t.Fatalf("expected cloud mode, got %s", h.s.mode)
	}
}

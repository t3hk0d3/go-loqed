package gateway

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/bridge"
	"github.com/t3hk0d3/go-loqed/cloud"
	"github.com/t3hk0d3/go-loqed/internal/config"
	"github.com/t3hk0d3/go-loqed/internal/model"
	"github.com/t3hk0d3/go-loqed/internal/store"
)

type BridgeAPI interface {
	Status(ctx context.Context) (*bridge.Status, error)
	Command(ctx context.Context, a bridge.Action) error
	ListWebhooks(ctx context.Context) ([]bridge.Webhook, error)
	CreateWebhook(ctx context.Context, url string, t bridge.Triggers) error
	DeleteWebhook(ctx context.Context, id int) error
}

// CloudSource is the supervisor's view of the CloudHub.
type CloudSource interface {
	Locks(ctx context.Context, p Priority, notBefore time.Time) (LockList, error)
	Command(ctx context.Context, lockID string, s loqed.BoltState) error
}

type Publisher interface {
	PublishState(lockID string, s model.State) error
	PublishEvent(lockID string, e model.Event) error
	PublishAvailability(lockID string, online bool) error
	PublishCommandStatus(lockID string, s model.CommandStatus) error
	PublishWebhooks(lockID string, l model.WebhookList) error
	PublishWebhooksResult(lockID string, r model.WebhooksResult) error
}

type Prober func(ctx context.Context, address string) error

// TCPProbe checks an address is reachable without an HTTP request.
func TCPProbe(ctx context.Context, address string) error {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", address)
	if err != nil {
		return errors.Join(loqed.ErrUnreachable, err)
	}
	return conn.Close()
}

// BridgeAddress returns host:port for a bridge IP (port 80 unless given).
func BridgeAddress(bridgeIP string) string {
	if _, _, err := net.SplitHostPort(bridgeIP); err == nil {
		return bridgeIP
	}
	return net.JoinHostPort(bridgeIP, "80")
}

type BridgeEventMsg struct{ Event bridge.Event }

// RecordMsg replaces the lock's credentials (after a runtime refresh).
type RecordMsg struct{ Record store.LockRecord }
type CloudEventMsg struct{ Event cloud.WebhookEvent }
type CommandMsg struct {
	Command model.Command
	ID      string    // optional client id echoed in command_status
	At      time.Time // MQTT arrival time; deadlines count from here
}

var errNoBridge = errors.New("gateway: no usable bridge client")

type Deps struct {
	Publisher     Publisher
	Cloud         CloudSource
	Refresh       func(ctx context.Context, lockID string, reason Reason) (store.LockRecord, error)
	NewBridge     func(rec store.LockRecord) (BridgeAPI, error)
	Probe         Prober                          // bridge TCP liveness
	ProbeCloud    func(ctx context.Context) error // cloud host TCP reachability (unbudgeted)
	WebhookURL    func(rec store.LockRecord) (string, error)
	CloudWebhooks bool // webhook.public_url is set
	// SaveCloudWebhookID persists the numeric cloud lock id first seen on a
	// lock's cloud webhook URL (nil: keep it in memory only).
	SaveCloudWebhookID func(lockID, id string) error
	// TokenExpiry reports when the cloud token in use expires (nil or
	// false: unknown).
	TokenExpiry func() (time.Time, bool)
	Now         func() time.Time
	Log         *slog.Logger
}

type Timing struct {
	Liveness          time.Duration // bridge and cloud TCP probes
	Reconcile         time.Duration // max interval between /status (local) or reconcile polls (cloud push)
	OfflineRetry      time.Duration
	UnknownRecheck    time.Duration
	WebhookConfirm    time.Duration // /status if no matching webhook arrives
	StatusRecheck     time.Duration // one more /status after an unresolved confirmation read
	StatusEventWindow time.Duration // /status never overrides a bolt webhook this recent
	StatusMoveWindow  time.Duration // a /status read this soon after a movement may still show the old state
	WebhookRetry      time.Duration // retry bridge webhook registration
	CloudConfirm      time.Duration
	CloudPoll         time.Duration // how often cloud mode asks the budget for a poll
	CloudPollSpacing  time.Duration // budget spacing of background polls (12h / cloud_budget)
	StaleGrace        time.Duration
	DuplicateWindow   time.Duration // a repeat of the latest event within this is a duplicate (event_dedup_window); 0 = off
	PairingWindow     time.Duration // the other feed's copy of a published event is recognized this long
	GatewayWindow     time.Duration // events with the gateway key this soon after a command are the gateway's
	CommandDeadline   time.Duration // LOCK/UNLOCK: no attempt starts later than this after arrival
	OpenDeadline      time.Duration // OPEN: same
	LocalCutoff       time.Duration // LOCK/UNLOCK: local retries stop this long before the deadline
	OpenLocalCutoff   time.Duration // OPEN: same
	AlreadyThere      time.Duration // accepted in the target state: confirmed if nothing moves this long
	RequestTimeout    time.Duration
	FailureThreshold  int
}

func DefaultTiming(liveness, reconcile, pollSpacing time.Duration) Timing {
	return Timing{
		Liveness: liveness, Reconcile: reconcile,
		OfflineRetry: 5 * time.Minute, UnknownRecheck: 10 * time.Minute,
		WebhookConfirm: 30 * time.Second, StatusRecheck: time.Minute, WebhookRetry: 10 * time.Minute,
		StatusEventWindow: 5 * time.Minute, StatusMoveWindow: 3 * time.Minute,
		CloudConfirm: 5 * time.Second, CloudPoll: time.Minute, CloudPollSpacing: pollSpacing,
		StaleGrace:      10 * time.Minute,
		DuplicateWindow: 10 * time.Second, PairingWindow: 5 * time.Minute, GatewayWindow: time.Minute,
		CommandDeadline: 30 * time.Second, OpenDeadline: 10 * time.Second,
		LocalCutoff: 10 * time.Second, OpenLocalCutoff: 3 * time.Second, AlreadyThere: 5 * time.Second,
		RequestTimeout:   5 * time.Second,
		FailureThreshold: 3,
	}
}

type Health struct {
	Mode        model.Mode `json:"mode"`
	Available   bool       `json:"available"`
	LastEventAt *time.Time `json:"last_event_at"`
}

const warnRepeatWindow = 10 * time.Minute

// maxOtherWebhooks: more webhook targets than this on a bridge noticeably
// delay events and /status (the bridge delivers them one after another).
const maxOtherWebhooks = 3

type warnState struct {
	last       time.Time
	suppressed int
}

// Supervisor owns one lock. All fields below mu are only touched by the
// Run goroutine (or directly by tests).
type Supervisor struct {
	id      string
	d       Deps
	t       Timing
	log     *slog.Logger
	setting config.LockSetting
	in      chan any

	mu     sync.Mutex // guards rec and health, read from other goroutines
	rec    store.LockRecord
	health Health

	bridge BridgeAPI // nil when it cannot be built; never called while nil
	mode   model.Mode
	state  model.State

	probeFailures      int // bridge TCP probe
	httpFailures       int // bridge HTTP requests
	cloudProbeFailures int
	cloudAPIFailures   int

	nextProbe        time.Time
	nextCloudProbe   time.Time
	nextReconcile    time.Time
	nextCloudPoll    time.Time
	nextOfflineRetry time.Time
	lastUnknownCheck time.Time

	webhookOK        bool
	nextWebhookRetry time.Time

	// Bridge webhook list (spec 5.9).
	hooks    []bridge.Webhook   // the bridge's webhooks behind hookList, in id order
	hookList *model.WebhookList // last published list; nil until one was read

	// SetWebhooks requests (spec 5.9): one runs at a time, a step per call.
	hookJob   *webhookJob
	hookQueue [][]byte // raw requests waiting behind hookJob or a command

	// Bridge webhook delivery: registration only proves the gateway reaches
	// the bridge, not the reverse (a firewall may block bridge → gateway).
	webhookConfirmed    bool      // a signed bridge webhook arrived since the last (re-)registration
	lastBridgeEventAt   time.Time // last signed bridge webhook, duplicates included
	nextUnconfirmedRead time.Time // /status while delivery is unconfirmed
	bridgeCheckAt       time.Time // a bridge-sent command's confirmation window ends
	bridgeCheckSince    time.Time // ... and needs a bridge webhook since this
	bridgeCheckCmd      *command  // ... if it was confirmed (by any feed)

	// Command / movement confirmation.
	confirmTarget     loqed.BoltState // BoltUnknown: any reached state confirms
	confirmAt         time.Time       // local: /status at this time
	confirmViaCloud   bool            // if that /status fails, ask the cloud
	cloudConfirmAt    time.Time       // cloud confirmation poll at this time
	cloudConfirmSince time.Time       // only data fetched after this counts
	confirmRetried    bool            // the one extra confirmation poll was used
	confirmRechecked  bool            // the one extra confirmation /status read was used
	move              movement        // expected bolt movement (for /status hints)
	lastBoltEventAt   time.Time       // last webhook that set the bolt state

	lastFreshAt      time.Time // last fresh bolt data (status, poll, event)
	lastEventAt      time.Time // last applied lock event
	lastPollAt       time.Time // last successful cloud poll
	lastCloudEventAt time.Time

	published         []*lockEvent // lock events published within DuplicateWindow or PairingWindow (duplicate drop)
	lastCommandSentAt time.Time    // last gateway command written to the bridge or cloud

	warned map[string]*warnState

	cmds commandPipeline
}

func NewSupervisor(rec store.LockRecord, setting config.LockSetting, d Deps, t Timing) *Supervisor {
	s := &Supervisor{
		id: rec.ID, d: d, t: t, setting: setting, in: make(chan any, 64),
		log:    d.Log.With("lock", rec.Name, "lock_id", rec.ID),
		rec:    ApplySetting(rec, setting),
		mode:   model.ModeOffline,
		state:  model.State{BoltState: loqed.BoltUnknown, Mode: model.ModeOffline},
		warned: map[string]*warnState{},
	}
	s.cmds.s = s
	return s
}

func (s *Supervisor) ID() string { return s.id }

func (s *Supervisor) Record() store.LockRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rec
}

// BridgeKey returns the decoded bridge key for webhook verification.
func (s *Supervisor) BridgeKey() ([]byte, bool) {
	rec := s.Record()
	if rec.BridgeKey == "" {
		return nil, false
	}
	k, err := base64.StdEncoding.DecodeString(rec.BridgeKey)
	return k, err == nil
}

// ErrCloudIDMismatch: a cloud webhook on this lock's URL names another lock.
var ErrCloudIDMismatch = errors.New("gateway: cloud webhook is for a different lock")

// CloudIDMismatchError is the ErrCloudIDMismatch returned by BindCloudID. It
// carries both numeric ids so the warning can show what the lock is bound to:
// a wrong first webhook binds the lock for good.
type CloudIDMismatchError struct{ Bound, Got string }

func (e *CloudIDMismatchError) Error() string {
	return fmt.Sprintf("%v: this URL belongs to cloud lock %s, the webhook names %s", ErrCloudIDMismatch, e.Bound, e.Got)
}

func (e *CloudIDMismatchError) Unwrap() error { return ErrCloudIDMismatch }

// BoundCloudID returns the id a lock is bound to from a mismatch error, or "".
func BoundCloudID(err error) string {
	var m *CloudIDMismatchError
	if errors.As(err, &m) {
		return m.Bound
	}
	return ""
}

// BindCloudID checks the numeric lock id of a cloud webhook against the one
// learned from the first webhook on this lock's URL (and learns it then).
// It may be called from any goroutine.
func (s *Supervisor) BindCloudID(id string) error {
	s.mu.Lock()
	switch s.rec.CloudWebhookID {
	case id:
		s.mu.Unlock()
		return nil
	case "":
		s.rec.CloudWebhookID = id
		s.mu.Unlock()
	default:
		bound := s.rec.CloudWebhookID
		s.mu.Unlock()
		return &CloudIDMismatchError{Bound: bound, Got: id}
	}
	s.log.Info("learned the cloud lock id from its first cloud webhook", "cloud_lock_id", id)
	if s.d.SaveCloudWebhookID != nil {
		if err := s.d.SaveCloudWebhookID(s.id, id); err != nil {
			s.log.Warn("could not save the cloud lock id", "err", err)
		}
	}
	return nil
}

// Deliver queues a message without blocking; false means the queue is full.
func (s *Supervisor) Deliver(msg any) bool {
	select {
	case s.in <- msg:
		return true
	default:
		return false
	}
}

func (s *Supervisor) Health() Health {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.health
}

func (s *Supervisor) Run(ctx context.Context) {
	s.start(ctx)
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	// Command retries can be due between ticks (0.5 s backoff).
	wake := time.NewTimer(time.Hour)
	wake.Stop()
	defer wake.Stop()
	for {
		at := s.cmds.nextWake()
		if s.webhooksReady() {
			at = s.d.Now() // the next SetWebhooks call
		}
		if !at.IsZero() {
			wake.Reset(max(at.Sub(s.d.Now()), 0))
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			s.tick(ctx)
		case <-wake.C:
			if ctx.Err() != nil {
				continue
			}
			// Queued messages first: a command must not wait behind more
			// than the SetWebhooks call already running.
			select {
			case m := <-s.in:
				s.handle(ctx, m)
				continue
			default:
			}
			s.cmds.step(ctx, s.d.Now())
			s.stepWebhooks(ctx)
		case m := <-s.in:
			s.handle(ctx, m)
		}
	}
}

func (s *Supervisor) start(ctx context.Context) {
	if s.tryEnterLocal(ctx) {
		return
	}
	s.enterCloud(ctx)
}

func (s *Supervisor) tick(ctx context.Context) {
	if ctx.Err() != nil {
		return // shutting down: no new requests
	}
	now := s.d.Now()
	if !s.cloudConfirmAt.IsZero() && !now.Before(s.cloudConfirmAt) {
		s.cloudConfirmAt = time.Time{}
		s.pollCloud(ctx, PriorityConfirm, s.cloudConfirmSince)
	}
	switch s.mode {
	case model.ModeLocal:
		s.tickLocal(ctx, now)
	case model.ModeCloud:
		s.tickCloud(ctx, now)
	case model.ModeOffline:
		s.tickOffline(ctx, now)
	}
	s.cmds.step(ctx, s.d.Now())
	s.stepWebhooks(ctx)
}

func (s *Supervisor) handle(ctx context.Context, m any) {
	switch m := m.(type) {
	case BridgeEventMsg:
		s.onBridgeEvent(ctx, m.Event)
	case CloudEventMsg:
		s.onCloudEvent(ctx, m.Event)
	case CommandMsg:
		s.cmds.Submit(ctx, s.d.Now(), m.Command, m.ID, m.At)
	case RecordMsg:
		s.setRecord(m.Record)
		s.ensureBridge()
	case WebhooksRequestMsg:
		s.onWebhooksRequest(ctx, m.Body)
	}
}

func (s *Supervisor) reqCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, s.t.RequestTimeout)
}

func (s *Supervisor) available() bool { return s.mode != model.ModeOffline && s.state.LockOnline }

// publish sends the state document and availability, and updates Health.
func (s *Supervisor) publish() {
	s.state.TokenExpiresAt = nil
	if s.d.TokenExpiry != nil {
		if exp, ok := s.d.TokenExpiry(); ok {
			s.state.TokenExpiresAt = &exp
		}
	}
	if err := s.d.Publisher.PublishState(s.id, s.state); err != nil {
		s.log.Warn("publishing state failed", "err", err)
	}
	avail := s.available()
	if err := s.d.Publisher.PublishAvailability(s.id, avail); err != nil {
		s.log.Warn("publishing availability failed", "err", err)
	}
	s.mu.Lock()
	s.health = Health{Mode: s.mode, Available: avail, LastEventAt: s.state.LastEventAt}
	s.mu.Unlock()
}

func (s *Supervisor) setMode(m model.Mode) {
	if s.mode != m {
		s.log.Info("connection mode changed", "from", s.mode, "to", m)
	}
	s.mode, s.state.Mode = m, m
	s.probeFailures, s.httpFailures, s.cloudProbeFailures, s.cloudAPIFailures = 0, 0, 0, 0
}

func (s *Supervisor) setRecord(rec store.LockRecord) {
	s.mu.Lock()
	if rec.CloudWebhookID == "" {
		rec.CloudWebhookID = s.rec.CloudWebhookID
	}
	s.rec = ApplySetting(rec, s.setting)
	s.mu.Unlock()
	s.bridge = nil
}

func (s *Supervisor) markStale() {
	if !s.state.StateStale {
		s.state.StateStale = true
		s.publish()
	}
}

// warn logs at warn level, but repeats of the same message within 10 min
// are only counted and reported with the next emitted line.
func (s *Supervisor) warn(msg string, args ...any) {
	now := s.d.Now()
	w := s.warned[msg]
	if w != nil && now.Sub(w.last) < warnRepeatWindow {
		w.suppressed++
		return
	}
	if w != nil && w.suppressed > 0 {
		args = append(args, "repeated", w.suppressed)
	}
	s.warned[msg] = &warnState{last: now}
	s.log.Warn(msg, args...)
}

// recordEvent applies a lock event, publishes state and the HA event.
func (s *Supervisor) recordEvent(now time.Time, eventType string, key *int, cloudKeyName string, t model.Transition) {
	s.state.Apply(t)
	if t.SetBolt || t.SetLock {
		s.state.StateStale = false
		s.lastFreshAt = now
	}
	if t.SetBolt {
		s.lastBoltEventAt = now
	}
	s.lastEventAt = now
	name := s.keyName(key, cloudKeyName)
	at := now.UTC().Truncate(time.Second)
	s.state.LastEvent, s.state.LastKeyID, s.state.LastKeyName, s.state.LastEventAt = eventType, key, name, &at
	s.publish()
	ev := model.Event{EventType: t.Event, Reason: eventType, KeyLocalID: key, KeyName: name}
	if s.isGatewayKey(key) && s.gatewayActive(now) {
		ev.Source = model.Ptr(model.SourceGateway)
	}
	if err := s.d.Publisher.PublishEvent(s.id, ev); err != nil {
		s.log.Warn("publishing event failed", "err", err)
	}
}

// onReached clears a pending confirmation that this state satisfies; a
// motor stall schedules one /status check to learn the real position.
func (s *Supervisor) onReached(now time.Time, bolt loqed.BoltState, jammed bool) {
	if jammed {
		s.awaitConfirm(now, loqed.BoltUnknown)
		return
	}
	if s.confirmTarget == loqed.BoltUnknown || s.confirmTarget == bolt {
		s.confirmAt, s.cloudConfirmAt, s.confirmViaCloud = time.Time{}, time.Time{}, false
	}
}

// awaitConfirm expects a STATE_CHANGED_* event reaching target within
// WebhookConfirm; otherwise /status is checked (local mode only).
func (s *Supervisor) awaitConfirm(now time.Time, target loqed.BoltState) {
	if s.mode != model.ModeLocal {
		return
	}
	s.confirmTarget = target
	s.confirmAt = now.Add(s.t.WebhookConfirm)
	s.confirmViaCloud = false
	s.confirmRechecked = false
}

// scheduleCloudConfirm polls the cloud CloudConfirm after a cloud command,
// accepting only data fetched after the command.
func (s *Supervisor) scheduleCloudConfirm(now time.Time, target loqed.BoltState) {
	s.confirmTarget = target
	s.cloudConfirmAt = now.Add(s.t.CloudConfirm)
	s.cloudConfirmSince = now
	s.confirmRetried = false
}

// commandFailed reports a command failure to HA as a command_failed event.
func (s *Supervisor) commandFailed(c model.Command, class string, err error) {
	s.log.Error("lock command failed", "command", c, "error_class", class, "err", err)
	ev := model.Event{EventType: model.EventCommandFailed, Reason: string(c), Source: model.Ptr(model.SourceGateway), Error: class}
	if perr := s.d.Publisher.PublishEvent(s.id, ev); perr != nil {
		s.log.Warn("publishing event failed", "err", perr)
	}
}

// keyName prefers lock_settings key_names, then the cloud's key_name_user.
func (s *Supervisor) keyName(key *int, cloudName string) *string {
	if key != nil {
		if n := s.setting.KeyNames[*key]; n != "" {
			return &n
		}
	}
	if cloudName != "" {
		return &cloudName
	}
	return nil
}

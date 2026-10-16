package mqtt

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net"
	"net/url"
	"os"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/eclipse/paho.mqtt.golang/packets"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/loqed-mqtt/internal/model"
)

type ClientConfig struct {
	URL      string
	Username string
	Password loqed.Secret
	ClientID string
	Topics   Topics
	// Discovery publishes Home Assistant discovery; nil disables it.
	Discovery Discovery
	Now       func() time.Time // clock for command timestamps; nil = time.Now
	// CloudWebhooks subscribes to <base>/+/cloud_webhook (mqtt.cloud_webhooks).
	CloudWebhooks bool
	// BridgeWebhookControl subscribes to <base>/+/webhooks/set
	// (mqtt.bridge_webhook_control).
	BridgeWebhookControl bool
	// OnRemovedCleared is called with the ids of removed locks whose retained
	// topics have been cleared on the broker.
	OnRemovedCleared func(ids []string)
}

// Discovery describes a discovery protocol (Home Assistant) without the
// client knowing about it: where each lock's retained discovery document goes,
// what it contains, and which topic announces that the consumer restarted.
type Discovery interface {
	BirthTopic() string
	Topic(lockID string) string
	Payload(l LockInfo) ([]byte, error)
}

// LockInfo is what discovery needs to know about a lock.
type LockInfo struct {
	ID      string
	Name    string
	Model   string
	MacWifi string
}

// CloudWebhook is a LOQED cloud webhook body relayed over MQTT (for example
// by a Home Assistant automation). Body is passed on unchanged and must never
// be logged: it carries personal data.
type CloudWebhook struct {
	LockID string // the real lock id, not the topic id
	Body   []byte
}

// maxCloudWebhook matches the HTTP cloud webhook body limit.
const maxCloudWebhook = 64 << 10

// WebhooksRequest is a raw SetWebhooks request for the gateway to check. Body
// holds webhook URLs and is never logged.
type WebhooksRequest struct {
	LockID string // the real lock id, not the topic id
	Body   []byte
}

// Command is a lock command received over MQTT.
type Command struct {
	LockID  string
	Command model.Command
	ID      string // optional client id echoed in command_status
	At      time.Time
}

type Client struct {
	cfg      ClientConfig
	log      *slog.Logger
	mc       paho.Client
	commands chan Command
	webhooks chan CloudWebhook
	hookReqs chan WebhooksRequest
	now      func() time.Time

	mu        sync.Mutex
	locks     map[string]LockInfo // by topic id
	removed   []string            // real lock ids
	states    map[string][]byte   // by real lock id
	cmdStatus map[string][]byte   // by real lock id
	hookLists map[string][]byte   // by real lock id
	avail     map[string]string   // by real lock id
	downSince time.Time           // zero while connected
	// failedConnects counts failed connect attempts since the last success;
	// connWarned rate-limits their warnings per reason.
	failedConnects int
	connWarned     map[string]*connWarn

	// pubWarned rate-limits failed-publish warnings per topic kind.
	pubWarned map[string]*connWarn

	// retainMu serializes "update cache + hand to paho" for one retained
	// per-lock topic with the reconnect republish, so an older document can
	// never be published after a newer one.
	retainMu sync.Mutex

	// closed ends the goroutines that watch publish outcomes.
	closed context.Context
	close  context.CancelFunc
}

// publishTimeout bounds the publishes that wait for their acknowledgement
// (gateway status, discovery, removed-lock clean-up).
const publishTimeout = 5 * time.Second

// outcomeTimeout: a lock publish without PUBACK after this long is reported.
const outcomeTimeout = 30 * time.Second

// writeTimeout bounds paho's handoff to its writer and each socket write, so
// a stuck connection blocks a publisher for at most this long.
const writeTimeout = 2 * time.Second

// connWarnWindow: repeats of the same connect failure within this window are
// only counted and reported with the next warning (like the gateway's
// rate-limited warnings).
const connWarnWindow = 10 * time.Minute

type connWarn struct {
	last       time.Time
	suppressed int
}

func NewClient(cfg ClientConfig, log *slog.Logger) *Client {
	c := &Client{cfg: cfg, log: log, commands: make(chan Command, 16), webhooks: make(chan CloudWebhook, 16),
		hookReqs: make(chan WebhooksRequest, 16), now: cfg.Now,
		locks: map[string]LockInfo{}, states: map[string][]byte{}, cmdStatus: map[string][]byte{}, hookLists: map[string][]byte{},
		avail: map[string]string{}}
	if c.now == nil {
		c.now = time.Now
	}
	c.downSince = c.now()
	c.closed, c.close = context.WithCancel(context.Background())
	opts := paho.NewClientOptions().
		AddBroker(cfg.URL).
		SetClientID(cfg.ClientID).
		SetUsername(cfg.Username).
		SetPassword(cfg.Password.Reveal()).
		SetCleanSession(true).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetConnectRetryInterval(5*time.Second).
		SetMaxReconnectInterval(time.Minute).
		SetKeepAlive(30*time.Second).
		SetWriteTimeout(writeTimeout).
		// In order: OPEN then LOCK must never reach the gateway as LOCK then
		// OPEN (latest command wins). Safe because no handler blocks.
		SetOrderMatters(true).
		SetWill(cfg.Topics.Status(), "offline", 1, true).
		SetOnConnectHandler(func(paho.Client) {
			c.onConnected()
			go c.onConnect()
		}).
		// Fired after every failed connect or reconnect attempt; without it a
		// wrong mqtt.url or password would only show as silence.
		SetConnectionNotificationHandler(func(_ paho.Client, n paho.ConnectionNotification) {
			if f, ok := n.(paho.ConnectionNotificationFailed); ok {
				c.onConnectFailed(f.Reason)
			}
		}).
		SetConnectionLostHandler(func(_ paho.Client, err error) {
			c.mu.Lock()
			c.downSince = c.now()
			c.mu.Unlock()
			log.Warn("MQTT connection lost", "err", err)
		})
	c.mc = paho.NewClient(opts)
	return c
}

// Start connects in the background; paho retries until it succeeds.
func (c *Client) Start() { c.mc.Connect() }

func (c *Client) Close() {
	c.close()
	if c.mc.IsConnectionOpen() {
		c.mc.Publish(c.cfg.Topics.Status(), 1, true, "offline").WaitTimeout(2 * time.Second)
	}
	c.mc.Disconnect(500)
}

func (c *Client) Connected() bool { return c.mc.IsConnectionOpen() }

// DisconnectedFor reports how long the broker connection has been down
// (0 while connected).
func (c *Client) DisconnectedFor() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.downSince.IsZero() || c.mc.IsConnectionOpen() {
		return 0
	}
	return c.now().Sub(c.downSince)
}

func (c *Client) Commands() <-chan Command { return c.commands }

// CloudWebhooks delivers relayed cloud webhook bodies (ClientConfig.CloudWebhooks).
func (c *Client) CloudWebhooks() <-chan CloudWebhook { return c.webhooks }

// WebhooksRequests delivers SetWebhooks requests (ClientConfig.BridgeWebhookControl).
func (c *Client) WebhooksRequests() <-chan WebhooksRequest { return c.hookReqs }

// SetLocks sets the published locks. removed lists lock ids whose retained
// topics (discovery, state, availability) must be cleared; removals accumulate
// across calls and are cleared again on every reconnect until they have been
// published (see ClientConfig.OnRemovedCleared).
func (c *Client) SetLocks(locks []LockInfo, removed []string) {
	c.mu.Lock()
	c.locks = make(map[string]LockInfo, len(locks))
	for _, l := range locks {
		c.locks[TopicID(l.ID)] = l
	}
	pending := slices.Clone(c.removed)
	for _, id := range removed {
		if !slices.Contains(pending, id) {
			pending = append(pending, id)
		}
	}
	c.removed = slices.DeleteFunc(pending, func(id string) bool { _, ok := c.locks[TopicID(id)]; return ok })
	for _, id := range removed {
		delete(c.states, id)
		delete(c.cmdStatus, id)
		delete(c.hookLists, id)
		delete(c.avail, id)
	}
	c.mu.Unlock()
	if c.Connected() {
		c.publishDiscovery()
	}
}

func (c *Client) PublishState(lockID string, s model.State) error {
	b, err := json.Marshal(s)
	if err != nil {
		return err
	}
	c.retainMu.Lock()
	defer c.retainMu.Unlock()
	c.mu.Lock()
	c.states[lockID] = b
	c.mu.Unlock()
	return c.publishAsync("state", c.cfg.Topics.State(TopicID(lockID)), true, b)
}

// PublishCommandStatus publishes the retained command_status document.
func (c *Client) PublishCommandStatus(lockID string, s model.CommandStatus) error {
	b, err := json.Marshal(s)
	if err != nil {
		return err
	}
	c.retainMu.Lock()
	defer c.retainMu.Unlock()
	c.mu.Lock()
	c.cmdStatus[lockID] = b
	c.mu.Unlock()
	return c.publishAsync("command_status", c.cfg.Topics.CommandStatus(TopicID(lockID)), true, b)
}

// PublishWebhooks publishes the retained bridge webhook list.
func (c *Client) PublishWebhooks(lockID string, l model.WebhookList) error {
	b, err := json.Marshal(l)
	if err != nil {
		return err
	}
	c.retainMu.Lock()
	defer c.retainMu.Unlock()
	c.mu.Lock()
	c.hookLists[lockID] = b
	c.mu.Unlock()
	return c.publishAsync("webhooks", c.cfg.Topics.Webhooks(TopicID(lockID)), true, b)
}

// PublishWebhooksResult publishes a SetWebhooks result (not retained).
func (c *Client) PublishWebhooksResult(lockID string, r model.WebhooksResult) error {
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	return c.publishAsync("webhooks_result", c.cfg.Topics.WebhooksResult(TopicID(lockID)), false, b)
}

func (c *Client) PublishEvent(lockID string, e model.Event) error {
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	return c.publishAsync("event", c.cfg.Topics.Event(TopicID(lockID)), false, b)
}

func (c *Client) PublishAvailability(lockID string, online bool) error {
	v := "offline"
	if online {
		v = "online"
	}
	c.retainMu.Lock()
	defer c.retainMu.Unlock()
	c.mu.Lock()
	if c.avail[lockID] == v {
		c.mu.Unlock()
		return nil
	}
	c.avail[lockID] = v
	c.mu.Unlock()
	return c.publishAsync("availability", c.cfg.Topics.Availability(TopicID(lockID)), true, []byte(v))
}

// publishAsync hands a QoS 1 message to paho without waiting for its PUBACK
// (lock publishes: the gateway must never wait on the broker). While
// disconnected it does nothing: retained state is republished from the
// caches on reconnect. A failure paho reports at once is returned; a later
// one is logged by watchPublish.
func (c *Client) publishAsync(kind, topic string, retain bool, payload []byte) error {
	if !c.mc.IsConnectionOpen() {
		return nil
	}
	tok := c.mc.Publish(topic, 1, retain, payload)
	select {
	case <-tok.Done():
		return tok.Error()
	default:
	}
	go c.watchPublish(kind, tok)
	return nil
}

// watchPublish waits for a publish's outcome and warns about a failure or a
// missing PUBACK. Nothing waits for it.
func (c *Client) watchPublish(kind string, tok paho.Token) {
	t := time.NewTimer(outcomeTimeout)
	defer t.Stop()
	select {
	case <-tok.Done():
		if err := tok.Error(); err != nil {
			c.warnPublish(kind, err.Error())
		}
	case <-t.C:
		c.warnPublish(kind, "no acknowledgement from the broker")
	case <-c.closed.Done():
	}
}

// warnPublish logs a failed publish of one topic kind, rate-limited like the
// connect warnings. The topic and payload are never logged.
func (c *Client) warnPublish(kind, why string) {
	now := c.now()
	c.mu.Lock()
	w := c.pubWarned[kind]
	if w != nil && now.Sub(w.last) < connWarnWindow {
		w.suppressed++
		c.mu.Unlock()
		return
	}
	args := []any{"kind", kind, "reason", why}
	if w != nil && w.suppressed > 0 {
		args = append(args, "repeated", w.suppressed)
	}
	if c.pubWarned == nil {
		c.pubWarned = map[string]*connWarn{}
	}
	c.pubWarned[kind] = &connWarn{last: now}
	c.mu.Unlock()
	c.log.Warn("publishing "+kind+" failed", args...)
}

// publish sends a QoS 1 message and waits for its acknowledgement (gateway
// status, discovery, removed-lock clean-up). While disconnected it does
// nothing.
func (c *Client) publish(topic string, retain bool, payload []byte) error {
	if !c.mc.IsConnectionOpen() {
		return nil
	}
	tok := c.mc.Publish(topic, 1, retain, payload)
	if !tok.WaitTimeout(publishTimeout) {
		return fmt.Errorf("mqtt: publishing to %s timed out", topic)
	}
	return tok.Error()
}

// onConnected records a successful connect and logs it, with the number of
// failed attempts before it.
func (c *Client) onConnected() {
	c.mu.Lock()
	c.downSince = time.Time{}
	failed := c.failedConnects
	c.failedConnects = 0
	c.connWarned = nil
	c.mu.Unlock()
	args := []any{"broker", BrokerAddr(c.cfg.URL)}
	if failed > 0 {
		args = append(args, "failed_attempts", failed)
	}
	c.log.Info("connected to MQTT broker", args...)
}

// onConnectFailed warns about a failed connect attempt. It logs only the
// reason class, never the error: errors may carry the broker URL.
func (c *Client) onConnectFailed(err error) {
	reason, check := connectFailure(err)
	now := c.now()
	c.mu.Lock()
	c.failedConnects++
	w := c.connWarned[reason]
	if w != nil && now.Sub(w.last) < connWarnWindow {
		w.suppressed++
		c.mu.Unlock()
		return
	}
	args := []any{"broker", BrokerAddr(c.cfg.URL), "reason", reason, "check", check}
	if w != nil && w.suppressed > 0 {
		args = append(args, "repeated", w.suppressed)
	}
	if c.connWarned == nil {
		c.connWarned = map[string]*connWarn{}
	}
	c.connWarned[reason] = &connWarn{last: now}
	c.mu.Unlock()
	c.log.Warn("cannot connect to MQTT broker; retrying", args...)
}

// connectFailure classifies a paho connect error into a reason and the
// settings to check.
func connectFailure(err error) (reason, check string) {
	var (
		dnsErr   *net.DNSError
		netErr   net.Error
		recErr   tls.RecordHeaderError
		alertErr tls.AlertError
		verErr   *tls.CertificateVerificationError
		authErr  x509.UnknownAuthorityError
		hostErr  x509.HostnameError
		certErr  x509.CertificateInvalidError
	)
	switch {
	case errors.Is(err, packets.ErrorRefusedBadUsernameOrPassword), errors.Is(err, packets.ErrorRefusedNotAuthorised):
		return "not authorized (bad username or password)", "mqtt.username and mqtt.password"
	case errors.Is(err, packets.ErrorRefusedIDRejected):
		return "client id rejected", "mqtt.client_id"
	case errors.As(err, &recErr), errors.As(err, &alertErr), errors.As(err, &verErr),
		errors.As(err, &authErr), errors.As(err, &hostErr), errors.As(err, &certErr):
		return "TLS", "mqtt.url (tcp:// or ssl://, host name) and the broker certificate"
	case errors.As(err, &dnsErr):
		return "host not found", "the host in mqtt.url"
	case errors.Is(err, syscall.ECONNREFUSED):
		return "connection refused", "mqtt.url (host and port) and that the broker is running"
	case errors.Is(err, os.ErrDeadlineExceeded), errors.As(err, &netErr) && netErr.Timeout():
		return "timeout", "mqtt.url and that the broker is reachable"
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, syscall.ECONNRESET):
		return "connection closed by broker", "mqtt.url (port, tcp:// or ssl://) and the broker log"
	default:
		return "other", "mqtt.url, mqtt.username and mqtt.password"
	}
}

func (c *Client) onConnect() {
	t := c.cfg.Topics
	// Subscribe before publishing anything: paho runs this handler before it
	// resumes its message store on a reconnect, and a QoS 1 publish made in
	// that window races with the resume (paho v1.5.1). The subscription round
	// trips let the resume finish first.
	c.waitSubscribe(t.CommandWildcard(), c.mc.Subscribe(t.CommandWildcard(), 1, c.onCommand))
	if c.cfg.CloudWebhooks {
		c.waitSubscribe(t.CloudWebhookWildcard(), c.mc.Subscribe(t.CloudWebhookWildcard(), 1, c.onCloudWebhook))
	}
	if c.cfg.BridgeWebhookControl {
		c.waitSubscribe(t.WebhooksSetWildcard(), c.mc.Subscribe(t.WebhooksSetWildcard(), 1, c.onWebhooksSet))
	}
	if d := c.cfg.Discovery; d != nil {
		c.waitSubscribe(d.BirthTopic(), c.mc.Subscribe(d.BirthTopic(), 1, func(_ paho.Client, m paho.Message) {
			if string(m.Payload()) == "online" {
				c.log.Info("discovery consumer came online; republishing discovery")
				go c.publishDiscovery()
			}
		}))
	}
	if err := c.publish(t.Status(), true, []byte("online")); err != nil {
		c.log.Warn("publishing gateway status failed", "err", err)
	}
	c.publishDiscovery()
	c.mu.Lock()
	avail, states := slices.Sorted(maps.Keys(c.avail)), slices.Sorted(maps.Keys(c.states))
	cmdStatus, hookLists := slices.Sorted(maps.Keys(c.cmdStatus)), slices.Sorted(maps.Keys(c.hookLists))
	c.mu.Unlock()
	for _, id := range avail {
		c.republish("availability", t.Availability(TopicID(id)), func() []byte {
			if v, ok := c.avail[id]; ok {
				return []byte(v)
			}
			return nil
		})
	}
	for _, id := range states {
		c.republish("state", t.State(TopicID(id)), func() []byte { return c.states[id] })
	}
	for _, id := range cmdStatus {
		c.republish("command_status", t.CommandStatus(TopicID(id)), func() []byte { return c.cmdStatus[id] })
	}
	for _, id := range hookLists {
		c.republish("webhooks", t.Webhooks(TopicID(id)), func() []byte { return c.hookLists[id] })
	}
}

// republish sends one retained topic's current cached value (read under
// c.mu by current), holding retainMu only for this topic's handoff: a value
// published meanwhile is either already cached and sent here, or handed to
// paho after this one.
func (c *Client) republish(kind, topic string, current func() []byte) {
	c.retainMu.Lock()
	defer c.retainMu.Unlock()
	c.mu.Lock()
	b := current()
	c.mu.Unlock()
	if b == nil {
		return // removed meanwhile
	}
	_ = c.publishAsync(kind, topic, true, b)
}

// waitSubscribe logs a warning if a subscription fails or times out.
func (c *Client) waitSubscribe(topic string, tok paho.Token) {
	if !tok.WaitTimeout(publishTimeout) {
		c.log.Warn("MQTT subscription timed out", "topic", topic)
	} else if err := tok.Error(); err != nil {
		c.log.Warn("MQTT subscription failed", "topic", topic, "err", err)
	}
}

// BrokerAddr reduces a broker URL to scheme://host:port, dropping any
// credentials, path and query.
func BrokerAddr(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return "<unparsable URL>"
	}
	return u.Scheme + "://" + u.Host
}

// publishDiscovery publishes discovery for current locks and clears the
// retained topics of removed ones (state/availability even without HA).
func (c *Client) publishDiscovery() {
	t := c.cfg.Topics
	c.mu.Lock()
	locks := make([]LockInfo, 0, len(c.locks))
	for _, l := range c.locks {
		locks = append(locks, l)
	}
	removed := append([]string(nil), c.removed...)
	c.mu.Unlock()
	var cleared []string
	for _, id := range removed {
		err := errors.Join(
			c.publish(t.State(TopicID(id)), true, []byte{}),
			c.publish(t.CommandStatus(TopicID(id)), true, []byte{}),
			c.publish(t.Webhooks(TopicID(id)), true, []byte{}),
			c.publish(t.Availability(TopicID(id)), true, []byte{}))
		if d := c.cfg.Discovery; d != nil {
			err = errors.Join(err, c.publish(d.Topic(id), true, []byte{}))
		}
		if err == nil && c.mc.IsConnectionOpen() {
			cleared = append(cleared, id)
		}
	}
	if len(cleared) > 0 {
		c.mu.Lock()
		c.removed = slices.DeleteFunc(c.removed, func(id string) bool { return slices.Contains(cleared, id) })
		c.mu.Unlock()
		if c.cfg.OnRemovedCleared != nil {
			c.cfg.OnRemovedCleared(cleared)
		}
	}
	d := c.cfg.Discovery
	if d == nil {
		return
	}
	for _, l := range locks {
		payload, err := d.Payload(l)
		if err != nil {
			c.log.Error("building discovery payload failed", "lock_id", l.ID, "err", err)
			continue
		}
		if err := c.publish(d.Topic(l.ID), true, payload); err != nil {
			c.log.Warn("publishing discovery failed", "lock_id", l.ID, "err", err)
		}
	}
}

func (c *Client) onCommand(_ paho.Client, m paho.Message) {
	if m.Retained() {
		// A retained OPEN would unlatch the door on every reconnect.
		c.log.Warn("retained command ignored; publish commands without the retain flag", "topic", m.Topic())
		return
	}
	l, ok := c.lockForTopic(m.Topic(), 1)
	if !ok {
		c.log.Warn("command for unknown lock ignored", "topic", m.Topic())
		return
	}
	cmd, id, err := model.ParseCommandMessage(m.Payload())
	if err != nil {
		c.log.Warn("invalid command ignored", "topic", m.Topic(), "err", err)
		return
	}
	select {
	case c.commands <- Command{LockID: l.ID, Command: cmd, ID: id, At: c.now()}:
	default:
		c.log.Warn("command queue full; command dropped", "lock_id", l.ID, "command", cmd)
	}
}

// lockForTopic returns the lock addressed by <base>/<topic id>/<leaf>, where
// the leaf has leafLevels topic levels.
func (c *Client) lockForTopic(topic string, leafLevels int) (LockInfo, bool) {
	parts := strings.Split(topic, "/")
	if len(parts) < leafLevels+1 {
		return LockInfo{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	l, ok := c.locks[parts[len(parts)-1-leafLevels]]
	return l, ok
}

func (c *Client) onCloudWebhook(_ paho.Client, m paho.Message) {
	if m.Retained() {
		// A retained body would be replayed as a new event on every reconnect.
		c.log.Warn("retained cloud webhook ignored; publish cloud webhooks without the retain flag", "topic", m.Topic())
		return
	}
	l, ok := c.lockForTopic(m.Topic(), 1)
	if !ok {
		c.log.Warn("cloud webhook for unknown lock ignored", "topic", m.Topic())
		return
	}
	if n := len(m.Payload()); n > maxCloudWebhook {
		c.log.Warn("oversized cloud webhook dropped", "lock_id", l.ID, "size", n)
		return
	}
	select {
	case c.webhooks <- CloudWebhook{LockID: l.ID, Body: bytes.Clone(m.Payload())}:
	default:
		c.log.Warn("cloud webhook queue full; webhook dropped", "lock_id", l.ID)
	}
}

func (c *Client) onWebhooksSet(_ paho.Client, m paho.Message) {
	if m.Retained() {
		// A retained request would be applied again on every reconnect.
		c.log.Warn("retained SetWebhooks request ignored; publish requests without the retain flag", "topic", m.Topic())
		return
	}
	l, ok := c.lockForTopic(m.Topic(), 2)
	if !ok {
		c.log.Warn("SetWebhooks request for unknown lock ignored", "topic", m.Topic())
		return
	}
	select {
	case c.hookReqs <- WebhooksRequest{LockID: l.ID, Body: bytes.Clone(m.Payload())}:
	default:
		c.log.Warn("SetWebhooks queue full; request dropped", "lock_id", l.ID)
	}
}

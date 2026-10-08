package mqtt

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"

	"github.com/t3hk0d3/go-loqed/internal/model"
)

type ClientConfig struct {
	URL      string
	Username string
	Password string
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

	// retainMu serializes "update cache + publish" for retained per-lock
	// topics with the reconnect republish, so an older document can never
	// be published after a newer one.
	retainMu sync.Mutex
}

const publishTimeout = 5 * time.Second

func NewClient(cfg ClientConfig, log *slog.Logger) *Client {
	c := &Client{cfg: cfg, log: log, commands: make(chan Command, 16), webhooks: make(chan CloudWebhook, 16),
		hookReqs: make(chan WebhooksRequest, 16), now: cfg.Now,
		locks: map[string]LockInfo{}, states: map[string][]byte{}, cmdStatus: map[string][]byte{}, hookLists: map[string][]byte{},
		avail: map[string]string{}}
	if c.now == nil {
		c.now = time.Now
	}
	c.downSince = c.now()
	opts := paho.NewClientOptions().
		AddBroker(cfg.URL).
		SetClientID(cfg.ClientID).
		SetUsername(cfg.Username).
		SetPassword(cfg.Password).
		SetCleanSession(true).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetConnectRetryInterval(5*time.Second).
		SetMaxReconnectInterval(time.Minute).
		SetKeepAlive(30*time.Second).
		// In order: OPEN then LOCK must never reach the gateway as LOCK then
		// OPEN (latest command wins). Safe because no handler blocks.
		SetOrderMatters(true).
		SetWill(cfg.Topics.Status(), "offline", 1, true).
		SetOnConnectHandler(func(paho.Client) {
			c.mu.Lock()
			c.downSince = time.Time{}
			c.mu.Unlock()
			go c.onConnect()
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
	return c.publish(c.cfg.Topics.State(TopicID(lockID)), true, b)
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
	return c.publish(c.cfg.Topics.CommandStatus(TopicID(lockID)), true, b)
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
	return c.publish(c.cfg.Topics.Webhooks(TopicID(lockID)), true, b)
}

// PublishWebhooksResult publishes a SetWebhooks result (not retained).
func (c *Client) PublishWebhooksResult(lockID string, r model.WebhooksResult) error {
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	return c.publish(c.cfg.Topics.WebhooksResult(TopicID(lockID)), false, b)
}

func (c *Client) PublishEvent(lockID string, e model.Event) error {
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	return c.publish(c.cfg.Topics.Event(TopicID(lockID)), false, b)
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
	return c.publish(c.cfg.Topics.Availability(TopicID(lockID)), true, []byte(v))
}

// publish sends a QoS 1 message. While disconnected it does nothing:
// retained state is republished from the caches on reconnect.
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

func (c *Client) onConnect() {
	t := c.cfg.Topics
	c.log.Info("connected to MQTT broker", "url", RedactURL(c.cfg.URL))
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
	c.retainMu.Lock()
	defer c.retainMu.Unlock()
	c.mu.Lock()
	states, cmdStatus, hookLists, avail := maps.Clone(c.states), maps.Clone(c.cmdStatus), maps.Clone(c.hookLists), maps.Clone(c.avail)
	c.mu.Unlock()
	for id, v := range avail {
		_ = c.publish(t.Availability(TopicID(id)), true, []byte(v))
	}
	for id, b := range states {
		_ = c.publish(t.State(TopicID(id)), true, b)
	}
	for id, b := range cmdStatus {
		_ = c.publish(t.CommandStatus(TopicID(id)), true, b)
	}
	for id, b := range hookLists {
		_ = c.publish(t.Webhooks(TopicID(id)), true, b)
	}
}

// waitSubscribe logs a warning if a subscription fails or times out.
func (c *Client) waitSubscribe(topic string, tok paho.Token) {
	if !tok.WaitTimeout(publishTimeout) {
		c.log.Warn("MQTT subscription timed out", "topic", topic)
	} else if err := tok.Error(); err != nil {
		c.log.Warn("MQTT subscription failed", "topic", topic, "err", err)
	}
}

// RedactURL hides a password embedded in a broker URL.
func RedactURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "<unparsable URL>"
	}
	return u.Redacted()
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

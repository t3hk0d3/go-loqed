package hass

import (
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

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/t3hk0d3/go-loqed/internal/model"
)

type ClientConfig struct {
	URL       string
	Username  string
	Password  string
	ClientID  string
	Topics    Topics
	HAEnabled bool
	Version   string
	Now       func() time.Time // clock for command timestamps; nil = time.Now
	// OnRemovedCleared is called with the ids of removed locks whose retained
	// topics have been cleared on the broker.
	OnRemovedCleared func(ids []string)
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
	mc       mqtt.Client
	commands chan Command
	now      func() time.Time

	mu        sync.Mutex
	locks     map[string]LockInfo // by topic id
	removed   []string            // real lock ids
	states    map[string][]byte   // by real lock id
	cmdStatus map[string][]byte   // by real lock id
	avail     map[string]string   // by real lock id
	downSince time.Time           // zero while connected

	// retainMu serializes "update cache + publish" for retained per-lock
	// topics with the reconnect republish, so an older document can never
	// be published after a newer one.
	retainMu sync.Mutex
}

const publishTimeout = 5 * time.Second

func NewClient(cfg ClientConfig, log *slog.Logger) *Client {
	c := &Client{cfg: cfg, log: log, commands: make(chan Command, 16), now: cfg.Now,
		locks: map[string]LockInfo{}, states: map[string][]byte{}, cmdStatus: map[string][]byte{}, avail: map[string]string{}}
	if c.now == nil {
		c.now = time.Now
	}
	c.downSince = c.now()
	opts := mqtt.NewClientOptions().
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
		SetOrderMatters(false).
		SetWill(cfg.Topics.Status(), "offline", 1, true).
		SetOnConnectHandler(func(mqtt.Client) {
			c.mu.Lock()
			c.downSince = time.Time{}
			c.mu.Unlock()
			go c.onConnect()
		}).
		SetConnectionLostHandler(func(_ mqtt.Client, err error) {
			c.mu.Lock()
			c.downSince = c.now()
			c.mu.Unlock()
			log.Warn("MQTT connection lost", "err", err)
		})
	c.mc = mqtt.NewClient(opts)
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
		return fmt.Errorf("hass: publishing to %s timed out", topic)
	}
	return tok.Error()
}

func (c *Client) onConnect() {
	t := c.cfg.Topics
	c.log.Info("connected to MQTT broker", "url", RedactURL(c.cfg.URL))
	if err := c.publish(t.Status(), true, []byte("online")); err != nil {
		c.log.Warn("publishing gateway status failed", "err", err)
	}
	c.waitSubscribe(t.CommandWildcard(), c.mc.Subscribe(t.CommandWildcard(), 1, c.onCommand))
	if c.cfg.HAEnabled {
		c.waitSubscribe(t.HAStatus(), c.mc.Subscribe(t.HAStatus(), 1, func(_ mqtt.Client, m mqtt.Message) {
			if string(m.Payload()) == "online" {
				c.log.Info("Home Assistant came online; republishing discovery")
				go c.publishDiscovery()
			}
		}))
	}
	c.publishDiscovery()
	c.retainMu.Lock()
	defer c.retainMu.Unlock()
	c.mu.Lock()
	states, cmdStatus, avail := maps.Clone(c.states), maps.Clone(c.cmdStatus), maps.Clone(c.avail)
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
}

// waitSubscribe logs a warning if a subscription fails or times out.
func (c *Client) waitSubscribe(topic string, tok mqtt.Token) {
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
			c.publish(t.Availability(TopicID(id)), true, []byte{}))
		if c.cfg.HAEnabled {
			err = errors.Join(err, c.publish(t.Discovery(TopicID(id)), true, []byte{}))
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
	if !c.cfg.HAEnabled {
		return
	}
	for _, l := range locks {
		payload, err := DiscoveryPayload(t, l, c.cfg.Version)
		if err != nil {
			c.log.Error("building discovery payload failed", "lock_id", l.ID, "err", err)
			continue
		}
		if err := c.publish(t.Discovery(TopicID(l.ID)), true, payload); err != nil {
			c.log.Warn("publishing discovery failed", "lock_id", l.ID, "err", err)
		}
	}
}

func (c *Client) onCommand(_ mqtt.Client, m mqtt.Message) {
	if m.Retained() {
		// A retained OPEN would unlatch the door on every reconnect.
		c.log.Warn("retained command ignored; publish commands without the retain flag", "topic", m.Topic())
		return
	}
	parts := strings.Split(m.Topic(), "/")
	if len(parts) < 2 {
		return
	}
	tid := parts[len(parts)-2]
	c.mu.Lock()
	l, ok := c.locks[tid]
	c.mu.Unlock()
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

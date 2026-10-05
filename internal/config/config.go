// Package config loads and validates loqed-mqtt settings.
package config

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// decodeStrict decodes a node rejecting unknown fields; Node.Decode inside an
// UnmarshalYAML method is not strict even when the outer decoder is.
func decodeStrict(n *yaml.Node, out any) error {
	b, err := yaml.Marshal(n)
	if err != nil {
		return err
	}
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	return dec.Decode(out)
}

// Duration is a time.Duration written as "60s", "24h"; "0" or "" is zero.
type Duration time.Duration

func (d Duration) D() time.Duration { return time.Duration(d) }

func (d *Duration) UnmarshalYAML(n *yaml.Node) error {
	var s string
	if err := n.Decode(&s); err != nil {
		return err
	}
	s = strings.TrimSpace(s)
	if s == "" || s == "0" {
		*d = 0
		return nil
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	*d = Duration(v)
	return nil
}

type Config struct {
	CloudToken        string          `yaml:"cloud_token"`
	CloudEmail        string          `yaml:"cloud_email"`
	CloudPassword     string          `yaml:"cloud_password"`
	Locks             []string        `yaml:"locks"`
	LockSettings      LockSettingsMap `yaml:"lock_settings"`
	CachePath         string          `yaml:"cache_path"`
	CacheMaxAge       Duration        `yaml:"cache_max_age"`
	ReconcileInterval Duration        `yaml:"reconcile_interval"`
	LivenessInterval  Duration        `yaml:"liveness_interval"`
	CloudBudget       int             `yaml:"cloud_budget"`
	Webhook           Webhook         `yaml:"webhook"`
	MQTT              MQTT            `yaml:"mqtt"`
	HomeAssistant     HomeAssistant   `yaml:"homeassistant"`
	LogLevel          string          `yaml:"log_level"`
	LogFormat         string          `yaml:"log_format"`
}

type Webhook struct {
	Listen      string `yaml:"listen"`
	PrivateURL  string `yaml:"private_url"`
	PublicURL   string `yaml:"public_url"`
	CloudSecret string `yaml:"cloud_secret"`
}

type MQTT struct {
	URL       string `yaml:"url"`
	Username  string `yaml:"username"`
	Password  string `yaml:"password"`
	ClientID  string `yaml:"client_id"`
	BaseTopic string `yaml:"base_topic"`
}

type HomeAssistant struct {
	Enabled         bool   `yaml:"enabled"`
	DiscoveryPrefix string `yaml:"discovery_prefix"`
}

// LockSetting overrides cloud data for one lock (keyed by lock id or name).
type LockSetting struct {
	BridgeIP  string   `yaml:"bridge_ip"`
	BridgeKey string   `yaml:"bridge_key"`
	KeySecret string   `yaml:"key_secret"`
	LocalID   *int     `yaml:"local_id"`
	KeyNames  KeyNames `yaml:"key_names"`
}

// LockSettingsMap accepts a mapping (YAML) or a list of entries with a
// "lock" field (HA add-on options cannot have free-form keys).
type LockSettingsMap map[string]LockSetting

func (m *LockSettingsMap) UnmarshalYAML(n *yaml.Node) error {
	if *m == nil {
		*m = LockSettingsMap{}
	}
	switch n.Kind {
	case yaml.MappingNode:
		var raw map[string]LockSetting
		if err := decodeStrict(n, &raw); err != nil {
			return err
		}
		for k, v := range raw {
			(*m)[k] = v
		}
	case yaml.SequenceNode:
		var list []struct {
			Lock        string `yaml:"lock"`
			LockSetting `yaml:",inline"`
		}
		if err := decodeStrict(n, &list); err != nil {
			return err
		}
		for _, e := range list {
			if e.Lock == "" {
				return errors.New("lock_settings: every entry needs a lock name or id")
			}
			(*m)[e.Lock] = e.LockSetting
		}
	case yaml.ScalarNode:
		if n.Tag != "!!null" {
			return errors.New("lock_settings must be a mapping or a list")
		}
	default:
		return errors.New("lock_settings must be a mapping or a list")
	}
	return nil
}

// KeyNames maps key_local_id to a display name. Accepts "1=Alice,3=Bob"
// (the add-on form), {1: Alice}, ["1=Alice"] or [{id: 1, name: Alice}].
// Map keys are parsed from strings because JSON keys always are.
type KeyNames map[int]string

func (k *KeyNames) UnmarshalYAML(n *yaml.Node) error {
	if *k == nil {
		*k = KeyNames{}
	}
	switch n.Kind {
	case yaml.MappingNode:
		var raw map[string]string
		if err := n.Decode(&raw); err != nil {
			return err
		}
		for idText, name := range raw {
			id, err := strconv.Atoi(strings.TrimSpace(idText))
			if err != nil {
				return fmt.Errorf("key_names key %q must be a number", idText)
			}
			(*k)[id] = name
		}
	case yaml.SequenceNode:
		for _, item := range n.Content {
			if item.Kind == yaml.ScalarNode {
				if err := k.addPair(item.Value); err != nil {
					return err
				}
				continue
			}
			var e struct {
				ID   *int   `yaml:"id"`
				Name string `yaml:"name"`
			}
			if err := decodeStrict(item, &e); err != nil {
				return err
			}
			if e.ID == nil {
				return errors.New("key_names entry needs an id")
			}
			(*k)[*e.ID] = e.Name
		}
	case yaml.ScalarNode:
		if n.Tag == "!!null" {
			return nil
		}
		for _, pair := range strings.Split(n.Value, ",") {
			if strings.TrimSpace(pair) == "" {
				continue
			}
			if err := k.addPair(pair); err != nil {
				return err
			}
		}
	default:
		return errors.New("key_names must be a string, a mapping or a list")
	}
	return nil
}

// addPair parses "1=Alice".
func (k KeyNames) addPair(pair string) error {
	idText, name, ok := strings.Cut(pair, "=")
	id, err := strconv.Atoi(strings.TrimSpace(idText))
	if !ok || err != nil {
		return fmt.Errorf("key_names entry %q must look like 1=Alice", strings.TrimSpace(pair))
	}
	k[id] = strings.TrimSpace(name)
	return nil
}

func Defaults() Config {
	return Config{
		CachePath:         "/data/locks.json",
		ReconcileInterval: Duration(24 * time.Hour),
		LivenessInterval:  Duration(60 * time.Second),
		CloudBudget:       10,
		Webhook:           Webhook{Listen: ":8099"},
		MQTT:              MQTT{ClientID: "loqed-mqtt", BaseTopic: "loqed"},
		HomeAssistant:     HomeAssistant{Enabled: true, DiscoveryPrefix: "homeassistant"},
		LogLevel:          "info",
		LogFormat:         "text",
	}
}

// fillDefaults restores defaults for settings explicitly set to empty.
func (c *Config) fillDefaults() {
	d := Defaults()
	if c.CachePath == "" {
		c.CachePath = d.CachePath
	}
	if c.ReconcileInterval == 0 {
		c.ReconcileInterval = d.ReconcileInterval
	}
	if c.LivenessInterval == 0 {
		c.LivenessInterval = d.LivenessInterval
	}
	if c.CloudBudget == 0 {
		c.CloudBudget = d.CloudBudget
	}
	if c.Webhook.Listen == "" {
		c.Webhook.Listen = d.Webhook.Listen
	}
	if c.MQTT.ClientID == "" {
		c.MQTT.ClientID = d.MQTT.ClientID
	}
	if c.MQTT.BaseTopic == "" {
		c.MQTT.BaseTopic = d.MQTT.BaseTopic
	}
	if c.HomeAssistant.DiscoveryPrefix == "" {
		c.HomeAssistant.DiscoveryPrefix = d.HomeAssistant.DiscoveryPrefix
	}
	if c.LogLevel == "" {
		c.LogLevel = d.LogLevel
	}
	if c.LogFormat == "" {
		c.LogFormat = d.LogFormat
	}
}

// HasCloudCredentials reports whether a token or a portal account is
// configured. An e-mail without a password can still use a cached minted
// token; see CanMint.
func (c Config) HasCloudCredentials() bool {
	return c.CloudToken != "" || c.CloudEmail != ""
}

// CanMint reports whether a new token can be minted via the portal.
func (c Config) CanMint() bool {
	return c.CloudEmail != "" && c.CloudPassword != ""
}

// Validate checks values that cannot be fixed at runtime.
func (c Config) Validate() error {
	var errs []error
	add := func(format string, a ...any) { errs = append(errs, fmt.Errorf(format, a...)) }

	if c.CloudPassword != "" && c.CloudEmail == "" {
		add("cloud_password needs cloud_email")
	}
	if c.CloudBudget < 1 || c.CloudBudget > 12 {
		add("cloud_budget must be between 1 and 12 (LOQED blocks accounts after 12 requests in 12h)")
	}
	if c.LivenessInterval.D() < 5*time.Second {
		add("liveness_interval must be at least 5s")
	}
	if c.ReconcileInterval.D() < time.Minute {
		add("reconcile_interval must be at least 1m")
	}
	if c.CacheMaxAge < 0 {
		add("cache_max_age must not be negative")
	}
	for name, u := range map[string]string{"webhook.private_url": c.Webhook.PrivateURL, "webhook.public_url": c.Webhook.PublicURL} {
		if u == "" {
			continue
		}
		pu, err := url.Parse(u)
		if err != nil || (pu.Scheme != "http" && pu.Scheme != "https") || pu.Host == "" {
			add("%s must be an absolute http(s) URL", name)
			continue
		}
		if strings.Trim(pu.Path, "/") != "" || pu.RawQuery != "" {
			add("%s must not have a path or query (the gateway serves /webhook/ and /cloud/ at the root)", name)
		}
	}
	if s := c.Webhook.CloudSecret; s != "" && len(s) < 16 {
		add("webhook.cloud_secret must be at least 16 characters")
	}
	if _, _, err := net.SplitHostPort(c.Webhook.Listen); err != nil {
		add("webhook.listen: %v", err)
	}
	if c.MQTT.BaseTopic == "" || strings.ContainsAny(c.MQTT.BaseTopic, "+#") {
		add("mqtt.base_topic must be non-empty and contain no wildcards")
	}
	if c.HomeAssistant.DiscoveryPrefix == "" || strings.ContainsAny(c.HomeAssistant.DiscoveryPrefix, "+#") {
		add("homeassistant.discovery_prefix must be non-empty and contain no wildcards")
	}
	switch c.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		add("log_level must be one of debug, info, warn, error")
	}
	if c.LogFormat != "text" && c.LogFormat != "json" {
		add("log_format must be text or json")
	}
	for name, s := range c.LockSettings {
		if s.BridgeIP != "" && net.ParseIP(s.BridgeIP) == nil {
			add("lock_settings[%s].bridge_ip must be an IP address (hostnames are not resolved)", name)
		}
		for field, v := range map[string]string{"bridge_key": s.BridgeKey, "key_secret": s.KeySecret} {
			if v == "" {
				continue
			}
			if _, err := base64.StdEncoding.DecodeString(v); err != nil {
				add("lock_settings[%s].%s is not valid base64", name, field)
			}
		}
		if s.LocalID != nil && (*s.LocalID < 0 || *s.LocalID > 255) {
			add("lock_settings[%s].local_id must be between 0 and 255", name)
		}
	}
	return errors.Join(errs...)
}

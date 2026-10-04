# loqed-mqtt Gateway Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `loqed-mqtt`: a gateway that exposes every LOQED lock on an account to MQTT / Home Assistant, local-bridge first with automatic cloud fallback, shipped as a Docker image and a Home Assistant add-on.

**Architecture:** `cmd/loqed-mqtt` only wires `internal/app`. `internal/config` loads settings, `internal/store` caches cloud credentials, `internal/auth` resolves/mints cloud tokens, `internal/model` holds the state document and the LOQED→HA mapping, `internal/hass` owns MQTT and discovery, `internal/gateway` owns the per-lock failover state machine and the cloud request budget, `internal/webhook` serves bridge/cloud webhooks and `/healthz`. Each lock runs in one supervisor goroutine; all inputs reach it over a channel, and its handlers are plain methods so tests drive them with a fake clock.

**Tech Stack:** Go 1.27, GoLoqed (this module), `gopkg.in/yaml.v3`, `github.com/eclipse/paho.mqtt.golang`, `github.com/mochi-mqtt/server/v2` (tests only), Docker buildx, GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-10-04-loqed-mqtt-gateway-design.md`

**Prerequisite:** `docs/superpowers/plans/2026-10-04-goloqed-library.md` is fully implemented (packages `loqed`, `bridge`, `cloud`, `cloud/portal`, `internal/transport`).

## Global Constraints

- Module path `github.com/t3hk0d3/go-loqed`, `go 1.27`.
- Config precedence: environment (`LOQED_` prefix, nesting `__`) > YAML file (`--config`) > add-on `/data/options.json`.
- Defaults: `cache_path=/data/locks.json`, `cache_max_age=0`, `reconcile_interval=24h`, `liveness_interval=60s`, `cloud_budget=10`, `webhook.listen=":8099"`, `mqtt.client_id=loqed-mqtt`, `mqtt.base_topic=loqed`, `homeassistant.enabled=true`, `homeassistant.discovery_prefix=homeassistant`, `log_level=info`.
- Cache file written atomically with mode `0600`.
- Cloud budget: at most `cloud_budget` `GET /api/locks/` per rolling 12 h, account-wide; on rate limit suspend cloud reads 12 h.
- Failover: 3 consecutive failures, 5 s request timeout, offline retry every 5 min forever, unknown-state status recheck at most every 10 min, command max age 10 s, webhook confirm 10 s, cloud confirm 5 s, cloud enrichment window 30 s, refresh throttle 5 min per lock+reason, re-mint at most once per hour.
- Bridge addressed by IP only (never hostnames/mDNS).
- MQTT topics: `<base>/status` (retained, LWT `offline`), `<base>/<id>/availability` (retained), `<base>/<id>/state` (retained JSON), `<base>/<id>/event` (**not** retained), `<base>/<id>/command` (subscribe QoS 1). Discovery `<prefix>/device/loqed_<id>/config`.
- Never log tokens, passwords, keys, signed commands, full cloud responses, cloud webhook bodies; the cloud webhook URL is logged once at startup only.
- Token name for minted tokens: `loqed-mqtt (<hostname>)`.
- Commit messages end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

- A password/token in an environment variable containing YAML-significant characters (`a: b #c`, `[x`, `007`) must be used verbatim (Task 1 env test).
- Home Assistant restarting (birth message `online` on `<prefix>/status`) or the MQTT broker restarting must re-create discovery and retained state without a gateway restart (Task 6 birth + reconnect test).
- A lock removed from the account or the allow-list must disappear from HA via an empty retained discovery payload (Task 14 integration test with a stale cache).
- A command sent the moment the bridge dies must still move the lock via the cloud in the same request (Task 11 fallback test).
- With `lock_settings.<lock>.bridge_ip` pinned, a dead bridge must go to cloud mode without a cloud credential refresh that could never change the IP (Task 10 override test).

---

## File Structure

```
internal/config/config.go        Config types, defaults, Validate, YAML forms of lock_settings/key_names
internal/config/load.go          Load(Sources): options.json < YAML < env
internal/config/supervisor.go    ResolveMQTT via Supervisor services API
internal/store/store.go          credential cache (Store, Cache, LockRecord)
internal/auth/auth.go            token Resolver, PortalMinter
internal/model/model.go          State, Event, Command, enums
internal/model/mapping.go        LOQED events → transitions, sources, key ids
internal/hass/topics.go          topic layout, TopicID
internal/hass/discovery.go       device discovery payload
internal/hass/client.go          paho client: publish, commands, birth handling
internal/testutil/mqtt.go        in-process broker + subscriber for tests
internal/gateway/budget.go       cloud request budget
internal/gateway/cloudhub.go     coalesced, budgeted, re-authenticating cloud access
internal/gateway/refresher.go    credential refresh into the store
internal/gateway/records.go      allow-list + lock_settings application
internal/gateway/supervisor.go   Supervisor core, deps, timing, publishing
internal/gateway/local.go        local mode, bridge events, liveness, reconcile
internal/gateway/cloudmode.go    cloud/offline modes, cloud polling, cloud events
internal/gateway/commands.go     command handling and fallback
internal/gateway/manager.go      dispatch to supervisors, health
internal/webhook/handler.go      HTTP routes
internal/webhook/urls.go         private/public URL building, source IP
internal/app/app.go              startup wiring
cmd/loqed-mqtt/main.go           flags, logger, signals, healthcheck subcommand
Dockerfile, .dockerignore, docker-compose.yml
addon/config.yaml, addon/build.yaml, addon/Dockerfile, addon/DOCS.md, addon/translations/en.yaml
repository.yaml, README.md, .golangci.yml
.github/workflows/ci.yml, .github/workflows/release.yml
```

---

### Task 1: Configuration

**Files:**
- Create: `internal/config/config.go`, `internal/config/load.go`, `internal/config/supervisor.go`, `internal/config/config_test.go`

**Interfaces:**
- Produces:
  - `type config.Duration time.Duration` with `UnmarshalYAML` and `func (Duration) D() time.Duration`
  - `type config.Config struct{ CloudToken, CloudEmail, CloudPassword string; Locks []string; LockSettings LockSettingsMap; CachePath string; CacheMaxAge, ReconcileInterval, LivenessInterval Duration; CloudBudget int; Webhook Webhook; MQTT MQTT; HomeAssistant HomeAssistant; LogLevel string }`
  - `type config.Webhook struct{ Listen, PrivateURL, PublicURL, CloudSecret string }`
  - `type config.MQTT struct{ URL, Username, Password, ClientID, BaseTopic string }`
  - `type config.HomeAssistant struct{ Enabled bool; DiscoveryPrefix string }`
  - `type config.LockSetting struct{ BridgeIP, BridgeKey, KeySecret string; LocalID *int; KeyNames KeyNames }`
  - `type config.LockSettingsMap map[string]LockSetting`, `type config.KeyNames map[int]string`
  - `func config.Defaults() Config`, `type config.Sources struct{ OptionsFile, ConfigFile string; Environ []string }`, `func config.Load(Sources) (Config, error)`
  - `func (Config) Validate() error`, `func (Config) HasCloudCredentials() bool`
  - `const config.SupervisorURL = "http://supervisor"`, `const config.DefaultMQTTURL = "tcp://localhost:1883"`
  - `func config.ResolveMQTT(ctx context.Context, c *Config, supervisorToken, supervisorURL string, hc *http.Client) error`

- [ ] **Step 1: Add the YAML dependency**

Run: `go get gopkg.in/yaml.v3@latest`

- [ ] **Step 2: Write failing tests**

`internal/config/config_test.go`:

```go
package config_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/t3hk0d3/go-loqed/internal/config"
)

func write(t *testing.T, name, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestDefaults(t *testing.T) {
	cfg, err := config.Load(config.Sources{OptionsFile: "/nonexistent/options.json"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.CachePath != "/data/locks.json" || cfg.ReconcileInterval.D() != 24*time.Hour || cfg.LivenessInterval.D() != time.Minute ||
		cfg.CloudBudget != 10 || cfg.Webhook.Listen != ":8099" || cfg.MQTT.ClientID != "loqed-mqtt" || cfg.MQTT.BaseTopic != "loqed" ||
		!cfg.HomeAssistant.Enabled || cfg.HomeAssistant.DiscoveryPrefix != "homeassistant" || cfg.LogLevel != "info" {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
}

func TestPrecedenceOptionsThenYAMLThenEnv(t *testing.T) {
	opts := write(t, "options.json", `{"cloud_token":"from-options","mqtt":{"base_topic":"opt","url":"tcp://opt:1883"},"cloud_budget":5}`)
	yml := write(t, "config.yaml", "mqtt:\n  base_topic: yaml\nliveness_interval: 30s\n")
	cfg, err := config.Load(config.Sources{OptionsFile: opts, ConfigFile: yml, Environ: []string{
		"LOQED_MQTT__BASE_TOPIC=env", "LOQED_CLOUD_BUDGET=7", "LOQED_HOMEASSISTANT__ENABLED=false", "PATH=/bin",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.CloudToken != "from-options" || cfg.MQTT.URL != "tcp://opt:1883" || cfg.MQTT.BaseTopic != "env" ||
		cfg.LivenessInterval.D() != 30*time.Second || cfg.CloudBudget != 7 || cfg.HomeAssistant.Enabled {
		t.Fatalf("unexpected: %+v", cfg)
	}
}

func TestEnvStringsAreVerbatim(t *testing.T) {
	cfg, err := config.Load(config.Sources{Environ: []string{
		"LOQED_CLOUD_PASSWORD=a: b #c", "LOQED_CLOUD_TOKEN=007", "LOQED_MQTT__PASSWORD=[x",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.CloudPassword != "a: b #c" || cfg.CloudToken != "007" || cfg.MQTT.Password != "[x" {
		t.Fatalf("unexpected: %q %q %q", cfg.CloudPassword, cfg.CloudToken, cfg.MQTT.Password)
	}
}

func TestEnvListsDurationsAndMaps(t *testing.T) {
	cfg, err := config.Load(config.Sources{Environ: []string{
		"LOQED_LOCKS=Front door, Back door",
		"LOQED_CACHE_MAX_AGE=168h",
		`LOQED_LOCK_SETTINGS={"Front door":{"bridge_ip":"192.168.1.50","key_names":{"1":"Alice"}}}`,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Locks) != 2 || cfg.Locks[1] != "Back door" || cfg.CacheMaxAge.D() != 168*time.Hour {
		t.Fatalf("unexpected: %+v", cfg)
	}
	s := cfg.LockSettings["Front door"]
	if s.BridgeIP != "192.168.1.50" || s.KeyNames[1] != "Alice" {
		t.Fatalf("lock settings: %+v", cfg.LockSettings)
	}
}

func TestEnvUnknownKeyFails(t *testing.T) {
	if _, err := config.Load(config.Sources{Environ: []string{"LOQED_NOPE=1"}}); err == nil {
		t.Fatal("expected error")
	}
}

func TestLockSettingsForms(t *testing.T) {
	mapForm := write(t, "map.yaml", `
lock_settings:
  Front door:
    bridge_ip: 192.168.1.50
    local_id: 2
    key_names: {1: Alice, 3: Bob}
`)
	listForm := write(t, "options.json", `{"lock_settings":[{"lock":"Front door","bridge_ip":"192.168.1.50","local_id":2,"key_names":["1=Alice","3=Bob"]}]}`)
	objList := write(t, "objlist.yaml", `
lock_settings:
  - lock: Front door
    bridge_ip: 192.168.1.50
    local_id: 2
    key_names: [{id: 1, name: Alice}, {id: 3, name: Bob}]
`)
	for _, src := range []config.Sources{{ConfigFile: mapForm}, {OptionsFile: listForm}, {ConfigFile: objList}} {
		cfg, err := config.Load(src)
		if err != nil {
			t.Fatal(err)
		}
		s, ok := cfg.LockSettings["Front door"]
		if !ok || s.BridgeIP != "192.168.1.50" || s.LocalID == nil || *s.LocalID != 2 || s.KeyNames[1] != "Alice" || s.KeyNames[3] != "Bob" {
			t.Fatalf("%+v: %+v", src, cfg.LockSettings)
		}
	}
}

func TestUnknownYAMLFieldFails(t *testing.T) {
	if _, err := config.Load(config.Sources{ConfigFile: write(t, "c.yaml", "cloud_tokn: x\n")}); err == nil {
		t.Fatal("expected error for typo")
	}
}

func TestValidate(t *testing.T) {
	ok := config.Defaults()
	ok.CloudToken = "t"
	if err := ok.Validate(); err != nil {
		t.Fatalf("defaults should validate: %v", err)
	}
	bad := config.Defaults()
	bad.CloudEmail = "me@example.com"
	bad.CloudBudget = 20
	bad.LivenessInterval = config.Duration(time.Second)
	bad.Webhook.PublicURL = "loqed.example.com"
	bad.Webhook.CloudSecret = "short"
	bad.MQTT.BaseTopic = "loqed/#"
	bad.LogLevel = "chatty"
	bad.LockSettings = config.LockSettingsMap{"x": {BridgeIP: "LOQED-aa.local", BridgeKey: "%%%"}}
	err := bad.Validate()
	if err == nil {
		t.Fatal("expected errors")
	}
	for _, want := range []string{"cloud_password", "cloud_budget", "liveness_interval", "public_url", "cloud_secret", "base_topic", "log_level", "bridge_ip", "bridge_key"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("missing %q in %v", want, err)
		}
	}
}

func TestHasCloudCredentials(t *testing.T) {
	c := config.Defaults()
	if c.HasCloudCredentials() {
		t.Fatal("no credentials expected")
	}
	c.CloudEmail, c.CloudPassword = "a", "b"
	if !c.HasCloudCredentials() {
		t.Fatal("email+password counts")
	}
}

func TestResolveMQTT(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/services/mqtt" || r.Header.Get("Authorization") != "Bearer sup" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_, _ = w.Write([]byte(`{"result":"ok","data":{"host":"core-mosquitto","port":1883,"ssl":false,"username":"addons","password":"pw"}}`))
	}))
	defer srv.Close()
	ctx := context.Background()

	c := config.Defaults()
	if err := config.ResolveMQTT(ctx, &c, "sup", srv.URL, srv.Client()); err != nil {
		t.Fatal(err)
	}
	if c.MQTT.URL != "tcp://core-mosquitto:1883" || c.MQTT.Username != "addons" || c.MQTT.Password != "pw" {
		t.Fatalf("got %+v", c.MQTT)
	}

	c = config.Defaults()
	c.MQTT.URL = "tcp://mine:1883"
	if err := config.ResolveMQTT(ctx, &c, "sup", srv.URL, srv.Client()); err != nil || c.MQTT.URL != "tcp://mine:1883" {
		t.Fatalf("explicit URL must win: %v %+v", err, c.MQTT)
	}

	c = config.Defaults()
	if err := config.ResolveMQTT(ctx, &c, "", srv.URL, srv.Client()); err != nil || c.MQTT.URL != config.DefaultMQTTURL {
		t.Fatalf("no supervisor → default: %v %+v", err, c.MQTT)
	}

	c = config.Defaults()
	if err := config.ResolveMQTT(ctx, &c, "wrong", srv.URL, srv.Client()); err == nil {
		t.Fatal("expected error")
	}
}
```

- [ ] **Step 3: Run to verify failure**

Run: `go test ./internal/config/`
Expected: FAIL — package has no non-test files.

- [ ] **Step 4: Implement `config.go`**

```go
// Package config loads and validates loqed-mqtt settings.
package config

import (
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
		if err := n.Decode(&raw); err != nil {
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
		if err := n.Decode(&list); err != nil {
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

// KeyNames maps key_local_id to a display name. Accepts {1: Alice},
// ["1=Alice"] or [{id: 1, name: Alice}].
type KeyNames map[int]string

func (k *KeyNames) UnmarshalYAML(n *yaml.Node) error {
	if *k == nil {
		*k = KeyNames{}
	}
	switch n.Kind {
	case yaml.MappingNode:
		var raw map[int]string
		if err := n.Decode(&raw); err != nil {
			return err
		}
		for id, name := range raw {
			(*k)[id] = name
		}
	case yaml.SequenceNode:
		for _, item := range n.Content {
			if item.Kind == yaml.ScalarNode {
				idText, name, ok := strings.Cut(item.Value, "=")
				id, err := strconv.Atoi(strings.TrimSpace(idText))
				if !ok || err != nil {
					return fmt.Errorf("key_names entry %q must look like 1=Alice", item.Value)
				}
				(*k)[id] = strings.TrimSpace(name)
				continue
			}
			var e struct {
				ID   int    `yaml:"id"`
				Name string `yaml:"name"`
			}
			if err := item.Decode(&e); err != nil {
				return err
			}
			(*k)[e.ID] = e.Name
		}
	case yaml.ScalarNode:
		if n.Tag != "!!null" {
			return errors.New("key_names must be a mapping or a list")
		}
	default:
		return errors.New("key_names must be a mapping or a list")
	}
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
}

// HasCloudCredentials reports whether a token or a portal login is configured.
func (c Config) HasCloudCredentials() bool {
	return c.CloudToken != "" || (c.CloudEmail != "" && c.CloudPassword != "")
}

// Validate checks values that cannot be fixed at runtime.
func (c Config) Validate() error {
	var errs []error
	add := func(format string, a ...any) { errs = append(errs, fmt.Errorf(format, a...)) }

	if (c.CloudEmail == "") != (c.CloudPassword == "") {
		add("cloud_email and cloud_password must be set together")
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
```

- [ ] **Step 5: Implement `load.go`**

```go
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"reflect"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// EnvPrefix marks environment variables that override settings.
// Nesting uses "__": LOQED_MQTT__BASE_TOPIC sets mqtt.base_topic.
const EnvPrefix = "LOQED_"

// Sources lists where settings come from, lowest precedence first.
type Sources struct {
	OptionsFile string   // HA add-on /data/options.json; skipped if missing
	ConfigFile  string   // --config YAML; must exist when set
	Environ     []string // os.Environ()
}

func Load(src Sources) (Config, error) {
	cfg := Defaults()
	if src.OptionsFile != "" {
		if err := decodeFile(src.OptionsFile, &cfg, true); err != nil {
			return Config{}, err
		}
	}
	if src.ConfigFile != "" {
		if err := decodeFile(src.ConfigFile, &cfg, false); err != nil {
			return Config{}, err
		}
	}
	if err := applyEnv(&cfg, src.Environ); err != nil {
		return Config{}, err
	}
	cfg.fillDefaults()
	return cfg, nil
}

// decodeFile overlays a YAML (or JSON, a YAML subset) file onto cfg.
func decodeFile(path string, cfg *Config, optional bool) error {
	b, err := os.ReadFile(path)
	if optional && errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	if err := dec.Decode(cfg); err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("config: %s: %w", path, err)
	}
	return nil
}

func applyEnv(cfg *Config, environ []string) error {
	for _, kv := range environ {
		k, v, ok := strings.Cut(kv, "=")
		if !ok || !strings.HasPrefix(k, EnvPrefix) {
			continue
		}
		path := strings.Split(strings.ToLower(strings.TrimPrefix(k, EnvPrefix)), "__")
		if err := setPath(reflect.ValueOf(cfg).Elem(), path, v); err != nil {
			return fmt.Errorf("config: %s: %w", k, err)
		}
	}
	return nil
}

var unmarshalerType = reflect.TypeFor[yaml.Unmarshaler]()

func setPath(v reflect.Value, path []string, raw string) error {
	if len(path) == 0 {
		return setValue(v, raw)
	}
	if v.Kind() != reflect.Struct {
		return fmt.Errorf("%q is not a section", path[0])
	}
	t := v.Type()
	for i := range t.NumField() {
		name, _, _ := strings.Cut(t.Field(i).Tag.Get("yaml"), ",")
		if name == path[0] {
			return setPath(v.Field(i), path[1:], raw)
		}
	}
	return fmt.Errorf("unknown setting %q", path[0])
}

// setValue assigns raw text by field type. Strings are taken verbatim so
// passwords containing YAML syntax stay intact; structured values (maps,
// durations) are parsed as YAML/JSON.
func setValue(v reflect.Value, raw string) error {
	if v.Kind() == reflect.Map || reflect.PointerTo(v.Type()).Implements(unmarshalerType) {
		return yaml.Unmarshal([]byte(raw), v.Addr().Interface())
	}
	switch v.Kind() {
	case reflect.String:
		v.SetString(raw)
	case reflect.Int:
		n, err := strconv.Atoi(strings.TrimSpace(raw))
		if err != nil {
			return fmt.Errorf("expected an integer: %w", err)
		}
		v.SetInt(int64(n))
	case reflect.Bool:
		b, err := strconv.ParseBool(strings.TrimSpace(raw))
		if err != nil {
			return fmt.Errorf("expected true or false: %w", err)
		}
		v.SetBool(b)
	case reflect.Slice:
		if v.Type().Elem().Kind() != reflect.String {
			return fmt.Errorf("unsupported list type %s", v.Type())
		}
		var out []string
		for _, p := range strings.Split(raw, ",") {
			if p = strings.TrimSpace(p); p != "" {
				out = append(out, p)
			}
		}
		v.Set(reflect.ValueOf(out))
	default:
		return fmt.Errorf("unsupported setting type %s", v.Type())
	}
	return nil
}
```

- [ ] **Step 6: Implement `supervisor.go`**

```go
package config

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
)

const (
	SupervisorURL  = "http://supervisor"
	DefaultMQTTURL = "tcp://localhost:1883"
)

// ResolveMQTT fills mqtt.url (and credentials if unset). An explicit URL
// wins; under the HA Supervisor the MQTT service is queried; otherwise
// DefaultMQTTURL is used.
func ResolveMQTT(ctx context.Context, c *Config, supervisorToken, supervisorURL string, hc *http.Client) error {
	if c.MQTT.URL != "" {
		return nil
	}
	if supervisorToken == "" {
		c.MQTT.URL = DefaultMQTTURL
		return nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, supervisorURL+"/services/mqtt", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+supervisorToken)
	resp, err := hc.Do(req)
	if err != nil {
		return fmt.Errorf("config: asking the Supervisor for MQTT settings: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("config: Supervisor MQTT service lookup failed with HTTP %d; install the Mosquitto add-on or set mqtt.url", resp.StatusCode)
	}
	var out struct {
		Result string `json:"result"`
		Data   struct {
			Host     string `json:"host"`
			Port     int    `json:"port"`
			SSL      bool   `json:"ssl"`
			Username string `json:"username"`
			Password string `json:"password"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return fmt.Errorf("config: Supervisor MQTT response: %w", err)
	}
	if out.Result != "ok" || out.Data.Host == "" {
		return errors.New("config: the Supervisor has no MQTT service; install the Mosquitto add-on or set mqtt.url")
	}
	scheme := "tcp"
	if out.Data.SSL {
		scheme = "ssl"
	}
	c.MQTT.URL = scheme + "://" + net.JoinHostPort(out.Data.Host, strconv.Itoa(out.Data.Port))
	if c.MQTT.Username == "" {
		c.MQTT.Username, c.MQTT.Password = out.Data.Username, out.Data.Password
	}
	return nil
}
```

- [ ] **Step 7: Run tests**

Run: `go test ./internal/config/ -v`
Expected: PASS. If `TestLockSettingsForms` fails on the inline struct, confirm the embedded field is written exactly `LockSetting \`yaml:",inline"\``.

- [ ] **Step 8: Commit**

```bash
git add go.mod go.sum internal/config
git commit -m "config: load settings from options.json, YAML and environment

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: Credential cache

**Files:**
- Create: `internal/store/store.go`, `internal/store/store_test.go`

**Interfaces:**
- Consumes: `cloud.Lock`.
- Produces:
  - `const store.Version = 1`
  - `type store.LockRecord struct{ ID, Name, ModelName, BridgeIP, BridgeHostname, BridgeMacWifi string; LocalID *int; KeySecret, BridgeKey, BackendKey string }` + `HasLocalCredentials() bool`
  - `func store.FromCloud(cloud.Lock) LockRecord`
  - `type store.MintedToken struct{ ID, Value string }`
  - `type store.Cache struct{ Version int; TokenSHA256 string; Minted *MintedToken; CloudSecret string; FetchedAt time.Time; Locks []LockRecord }` + `Find(key string) (LockRecord, bool)` (by id, then name)
  - `type store.Status int`: `StatusLoaded`, `StatusMissing`, `StatusCorrupt`
  - `func store.Open(path string) (*Store, Status, error)`; `(*Store).Snapshot() Cache` (deep copy); `(*Store).Update(func(*Cache)) error` (mutates, then writes atomically; the in-memory change is kept even if writing fails)
  - `func store.TokenHash(token string) string` (hex SHA-256)

- [ ] **Step 1: Write failing tests**

`internal/store/store_test.go`:

```go
package store_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/t3hk0d3/go-loqed/cloud"
	"github.com/t3hk0d3/go-loqed/internal/store"
)

func TestOpenMissingThenRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sub", "locks.json")
	st, status, err := store.Open(path)
	if err != nil || status != store.StatusMissing {
		t.Fatalf("status %v err %v", status, err)
	}
	id := 1
	err = st.Update(func(c *store.Cache) {
		c.TokenSHA256 = store.TokenHash("tok")
		c.Minted = &store.MintedToken{ID: "t1", Value: "secret"}
		c.FetchedAt = time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
		c.Locks = []store.LockRecord{{ID: "lock1", Name: "Front door", BridgeIP: "192.168.1.50", LocalID: &id, KeySecret: "a", BridgeKey: "b"}}
	})
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("mode %v", info.Mode().Perm())
	}
	entries, _ := os.ReadDir(filepath.Dir(path))
	if len(entries) != 1 {
		t.Fatalf("temp files left behind: %v", entries)
	}

	again, status, err := store.Open(path)
	if err != nil || status != store.StatusLoaded {
		t.Fatalf("status %v err %v", status, err)
	}
	c := again.Snapshot()
	if c.Version != store.Version || c.Minted.Value != "secret" || c.TokenSHA256 != store.TokenHash("tok") || len(c.Locks) != 1 {
		t.Fatalf("got %+v", c)
	}
	if r, ok := c.Find("Front door"); !ok || r.ID != "lock1" || !r.HasLocalCredentials() {
		t.Fatalf("find by name: %+v %v", r, ok)
	}
	if _, ok := c.Find("lock1"); !ok {
		t.Fatal("find by id")
	}
}

func TestOpenCorrupt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "locks.json")
	_ = os.WriteFile(path, []byte("{not json"), 0o600)
	st, status, err := store.Open(path)
	if err != nil || status != store.StatusCorrupt || len(st.Snapshot().Locks) != 0 {
		t.Fatalf("status %v err %v", status, err)
	}
	_ = os.WriteFile(path, []byte(`{"version":99}`), 0o600)
	if _, status, _ := store.Open(path); status != store.StatusCorrupt {
		t.Fatalf("unknown version should be corrupt, got %v", status)
	}
}

func TestSnapshotIsACopy(t *testing.T) {
	st, _, _ := store.Open(filepath.Join(t.TempDir(), "locks.json"))
	_ = st.Update(func(c *store.Cache) { c.Locks = []store.LockRecord{{ID: "a"}} })
	snap := st.Snapshot()
	snap.Locks[0].ID = "mutated"
	if st.Snapshot().Locks[0].ID != "a" {
		t.Fatal("snapshot shares memory with the store")
	}
}

func TestFromCloud(t *testing.T) {
	id := 2
	r := store.FromCloud(cloud.Lock{ID: "x", Name: "n", ModelName: "m", BridgeIP: "1.2.3.4", BridgeHostname: "h",
		BridgeMacWifi: "mac", LocalID: &id, KeySecret: "k", BridgeKey: "b", BackendKey: "bk"})
	if r.ID != "x" || r.ModelName != "m" || *r.LocalID != 2 || r.BackendKey != "bk" || r.BridgeMacWifi != "mac" {
		t.Fatalf("%+v", r)
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/store/`
Expected: FAIL — no non-test files.

- [ ] **Step 3: Implement**

`internal/store/store.go`:

```go
// Package store persists cloud lock credentials so restarts do not need
// the (rate-limited) cloud.
package store

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/fs"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/t3hk0d3/go-loqed/cloud"
)

const Version = 1

type LockRecord struct {
	ID             string `json:"id"`
	Name           string `json:"name"`
	ModelName      string `json:"model_name"`
	BridgeIP       string `json:"bridge_ip"`
	BridgeHostname string `json:"bridge_hostname"` // informational only, never resolved
	BridgeMacWifi  string `json:"bridge_mac_wifi"`
	LocalID        *int   `json:"local_id"`
	KeySecret      string `json:"key_secret"`
	BridgeKey      string `json:"bridge_key"`
	BackendKey     string `json:"backend_key"`
}

func (r LockRecord) HasLocalCredentials() bool {
	return r.BridgeIP != "" && r.BridgeKey != "" && r.KeySecret != "" && r.LocalID != nil
}

func FromCloud(l cloud.Lock) LockRecord {
	r := LockRecord{ID: l.ID, Name: l.Name, ModelName: l.ModelName, BridgeIP: l.BridgeIP, BridgeHostname: l.BridgeHostname,
		BridgeMacWifi: l.BridgeMacWifi, KeySecret: l.KeySecret, BridgeKey: l.BridgeKey, BackendKey: l.BackendKey}
	if l.LocalID != nil {
		v := *l.LocalID
		r.LocalID = &v
	}
	return r
}

type MintedToken struct {
	ID    string `json:"id"`
	Value string `json:"value"`
}

type Cache struct {
	Version     int          `json:"version"`
	TokenSHA256 string       `json:"token_sha256,omitempty"`
	Minted      *MintedToken `json:"minted_token,omitempty"`
	CloudSecret string       `json:"cloud_secret,omitempty"`
	FetchedAt   time.Time    `json:"fetched_at"`
	Locks       []LockRecord `json:"locks"`
}

// Find looks a lock up by id, then by name.
func (c Cache) Find(key string) (LockRecord, bool) {
	for _, r := range c.Locks {
		if r.ID == key {
			return r, true
		}
	}
	for _, r := range c.Locks {
		if r.Name == key {
			return r, true
		}
	}
	return LockRecord{}, false
}

func (c Cache) clone() Cache {
	out := c
	out.Locks = slices.Clone(c.Locks)
	for i := range out.Locks {
		if id := out.Locks[i].LocalID; id != nil {
			v := *id
			out.Locks[i].LocalID = &v
		}
	}
	if c.Minted != nil {
		m := *c.Minted
		out.Minted = &m
	}
	return out
}

type Status int

const (
	StatusLoaded Status = iota
	StatusMissing
	StatusCorrupt
)

type Store struct {
	path  string
	mu    sync.Mutex
	cache Cache
}

// Open loads the cache. Missing or corrupt files yield an empty cache and
// the matching Status; only unexpected I/O errors are returned.
func Open(path string) (*Store, Status, error) {
	s := &Store{path: path, cache: Cache{Version: Version}}
	b, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return s, StatusMissing, nil
	}
	if err != nil {
		return nil, 0, fmt.Errorf("store: %w", err)
	}
	var c Cache
	if json.Unmarshal(b, &c) != nil || c.Version != Version {
		return s, StatusCorrupt, nil
	}
	s.cache = c
	return s, StatusLoaded, nil
}

func (s *Store) Snapshot() Cache {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cache.clone()
}

// Update applies fn and persists the result.
func (s *Store) Update(fn func(*Cache)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	fn(&s.cache)
	s.cache.Version = Version
	return write(s.path, s.cache)
}

func write(path string, c Cache) error {
	b, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("store: %w", err)
	}
	f, err := os.CreateTemp(dir, ".locks-*.json")
	if err != nil {
		return fmt.Errorf("store: %w", err)
	}
	tmp := f.Name()
	defer os.Remove(tmp) // no-op after a successful rename
	if err := f.Chmod(0o600); err != nil {
		f.Close()
		return fmt.Errorf("store: %w", err)
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return fmt.Errorf("store: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("store: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("store: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("store: %w", err)
	}
	return nil
}

func TokenHash(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}
```

- [ ] **Step 4: Run tests**

Run: `gofmt -w internal/store && go test ./internal/store/ -v -race`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add internal/store
git commit -m "store: add atomic 0600 credential cache

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: Cloud token resolution and minting

**Files:**
- Create: `internal/auth/auth.go`, `internal/auth/auth_test.go`

**Interfaces:**
- Consumes: `store.Store`, `store.MintedToken`, `portal.Client`, `portal.Token`, `portal.TokenInfo`, `loqed.ErrUnauthorized`.
- Produces:
  - `var auth.ErrNoToken`
  - `type auth.Minter interface{ Mint(ctx context.Context) (store.MintedToken, error) }`
  - `type auth.PortalSession interface{ ListTokens(ctx) ([]portal.TokenInfo, error); CreateToken(ctx, name string) (portal.Token, error); RevokeToken(ctx, id string) error; Logout(ctx) error }`
  - `type auth.PortalMinter struct{ Login func(ctx context.Context, email, password string) (PortalSession, error); Email, Password, TokenName string }`
  - `func auth.NewPortalMinter(c *portal.Client, email, password, tokenName string) *PortalMinter`
  - `func auth.TokenName(hostname string) string` → `loqed-mqtt (<hostname>)`
  - `const auth.RemintInterval = time.Hour`
  - `func auth.NewResolver(configured string, minter Minter, st *store.Store, now func() time.Time, log *slog.Logger) *Resolver`
  - `func (*Resolver) Token(ctx) (string, error)`; `func (*Resolver) Invalidate(ctx, rejected string) (string, error)`

- [ ] **Step 1: Write failing tests**

`internal/auth/auth_test.go`:

```go
package auth_test

import (
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/cloud/portal"
	"github.com/t3hk0d3/go-loqed/internal/auth"
	"github.com/t3hk0d3/go-loqed/internal/store"
)

type fakeMinter struct {
	calls int
	err   error
}

func (m *fakeMinter) Mint(context.Context) (store.MintedToken, error) {
	m.calls++
	if m.err != nil {
		return store.MintedToken{}, m.err
	}
	return store.MintedToken{ID: "id", Value: "minted-" + string(rune('0'+m.calls))}, nil
}

func newStore(t *testing.T) *store.Store {
	t.Helper()
	st, _, err := store.Open(filepath.Join(t.TempDir(), "locks.json"))
	if err != nil {
		t.Fatal(err)
	}
	return st
}

var discard = slog.New(slog.DiscardHandler)

func TestConfiguredTokenWins(t *testing.T) {
	m := &fakeMinter{}
	r := auth.NewResolver("configured", m, newStore(t), time.Now, discard)
	tok, err := r.Token(context.Background())
	if err != nil || tok != "configured" || m.calls != 0 {
		t.Fatalf("%q %v calls=%d", tok, err, m.calls)
	}
	if _, err := r.Invalidate(context.Background(), "configured"); !errors.Is(err, loqed.ErrUnauthorized) {
		t.Fatalf("got %v", err)
	}
}

func TestCachedMintedTokenIsReused(t *testing.T) {
	st := newStore(t)
	_ = st.Update(func(c *store.Cache) { c.Minted = &store.MintedToken{ID: "x", Value: "cached"} })
	m := &fakeMinter{}
	tok, err := auth.NewResolver("", m, st, time.Now, discard).Token(context.Background())
	if err != nil || tok != "cached" || m.calls != 0 {
		t.Fatalf("%q %v calls=%d", tok, err, m.calls)
	}
}

func TestMintsAndPersists(t *testing.T) {
	st := newStore(t)
	m := &fakeMinter{}
	tok, err := auth.NewResolver("", m, st, time.Now, discard).Token(context.Background())
	if err != nil || tok != "minted-1" {
		t.Fatalf("%q %v", tok, err)
	}
	if got := st.Snapshot().Minted; got == nil || got.Value != "minted-1" {
		t.Fatalf("not persisted: %+v", got)
	}
}

func TestNoCredentials(t *testing.T) {
	if _, err := auth.NewResolver("", nil, newStore(t), time.Now, discard).Token(context.Background()); !errors.Is(err, auth.ErrNoToken) {
		t.Fatalf("got %v", err)
	}
}

func TestInvalidateRemintsAtMostHourly(t *testing.T) {
	now := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	m := &fakeMinter{}
	r := auth.NewResolver("", m, newStore(t), func() time.Time { return now }, discard)
	ctx := context.Background()
	first, _ := r.Token(ctx)
	second, err := r.Invalidate(ctx, first)
	if !errors.Is(err, loqed.ErrUnauthorized) || second != "" || m.calls != 1 {
		t.Fatalf("re-mint within the hour must be refused: %q %v calls=%d", second, err, m.calls)
	}
	now = now.Add(time.Hour)
	second, err = r.Invalidate(ctx, first)
	if err != nil || second != "minted-2" {
		t.Fatalf("%q %v", second, err)
	}
	// Someone already replaced the rejected token: return the new one without minting.
	third, err := r.Invalidate(ctx, first)
	if err != nil || third != "minted-2" || m.calls != 2 {
		t.Fatalf("%q %v calls=%d", third, err, m.calls)
	}
}

type fakeSession struct {
	tokens  []portal.TokenInfo
	revoked []string
	created []string
	logout  bool
}

func (s *fakeSession) ListTokens(context.Context) ([]portal.TokenInfo, error) { return s.tokens, nil }
func (s *fakeSession) CreateToken(_ context.Context, name string) (portal.Token, error) {
	s.created = append(s.created, name)
	return portal.Token{ID: "new", Name: name, Value: "pat"}, nil
}
func (s *fakeSession) RevokeToken(_ context.Context, id string) error {
	s.revoked = append(s.revoked, id)
	return nil
}
func (s *fakeSession) Logout(context.Context) error { s.logout = true; return nil }

func TestPortalMinterRevokesSameNamedTokens(t *testing.T) {
	sess := &fakeSession{tokens: []portal.TokenInfo{{ID: "old", Name: "loqed-mqtt (nas)"}, {ID: "keep", Name: "HA"}}}
	m := &auth.PortalMinter{
		Login: func(_ context.Context, email, password string) (auth.PortalSession, error) {
			if email != "me" || password != "pw" {
				t.Fatalf("credentials %q %q", email, password)
			}
			return sess, nil
		},
		Email: "me", Password: "pw", TokenName: auth.TokenName("nas"),
	}
	tok, err := m.Mint(context.Background())
	if err != nil || tok.ID != "new" || tok.Value != "pat" {
		t.Fatalf("%+v %v", tok, err)
	}
	if len(sess.revoked) != 1 || sess.revoked[0] != "old" || sess.created[0] != "loqed-mqtt (nas)" || !sess.logout {
		t.Fatalf("session %+v", sess)
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/auth/`
Expected: FAIL — no non-test files.

- [ ] **Step 3: Implement**

`internal/auth/auth.go`:

```go
// Package auth decides which LOQED cloud token to use and mints new ones
// through the Integrations portal when email/password are configured.
package auth

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/cloud/portal"
	"github.com/t3hk0d3/go-loqed/internal/store"
)

var ErrNoToken = errors.New("auth: no LOQED token available; set cloud_token, or cloud_email and cloud_password")

// RemintInterval limits how often a rejected minted token is replaced.
const RemintInterval = time.Hour

type Minter interface {
	Mint(ctx context.Context) (store.MintedToken, error)
}

// PortalSession is the part of *portal.Session the minter needs.
type PortalSession interface {
	ListTokens(ctx context.Context) ([]portal.TokenInfo, error)
	CreateToken(ctx context.Context, name string) (portal.Token, error)
	RevokeToken(ctx context.Context, id string) error
	Logout(ctx context.Context) error
}

type PortalMinter struct {
	Login     func(ctx context.Context, email, password string) (PortalSession, error)
	Email     string
	Password  string
	TokenName string
}

func NewPortalMinter(c *portal.Client, email, password, tokenName string) *PortalMinter {
	return &PortalMinter{
		Login: func(ctx context.Context, email, password string) (PortalSession, error) {
			s, err := c.Login(ctx, email, password)
			if err != nil {
				return nil, err
			}
			return s, nil
		},
		Email: email, Password: password, TokenName: tokenName,
	}
}

func TokenName(hostname string) string { return "loqed-mqtt (" + hostname + ")" }

// Mint logs in, revokes earlier tokens with the same name, creates a new
// one and logs out.
func (m *PortalMinter) Mint(ctx context.Context) (store.MintedToken, error) {
	s, err := m.Login(ctx, m.Email, m.Password)
	if err != nil {
		return store.MintedToken{}, err
	}
	defer s.Logout(context.WithoutCancel(ctx))
	existing, err := s.ListTokens(ctx)
	if err != nil {
		return store.MintedToken{}, err
	}
	for _, t := range existing {
		if t.Name == m.TokenName {
			if err := s.RevokeToken(ctx, t.ID); err != nil {
				return store.MintedToken{}, err
			}
		}
	}
	tok, err := s.CreateToken(ctx, m.TokenName)
	if err != nil {
		return store.MintedToken{}, err
	}
	return store.MintedToken{ID: tok.ID, Value: tok.Value}, nil
}

type Resolver struct {
	configured string
	minter     Minter
	store      *store.Store
	now        func() time.Time
	log        *slog.Logger

	mu       sync.Mutex
	lastMint time.Time
}

func NewResolver(configured string, minter Minter, st *store.Store, now func() time.Time, log *slog.Logger) *Resolver {
	return &Resolver{configured: configured, minter: minter, store: st, now: now, log: log}
}

// Token returns cloud_token, else the cached minted token, else mints one.
func (r *Resolver) Token(ctx context.Context) (string, error) {
	if r.configured != "" {
		return r.configured, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if m := r.store.Snapshot().Minted; m != nil && m.Value != "" {
		return m.Value, nil
	}
	return r.mintLocked(ctx)
}

// Invalidate reports that rejected was refused by the cloud and returns a
// replacement, minting at most once per RemintInterval.
func (r *Resolver) Invalidate(ctx context.Context, rejected string) (string, error) {
	if r.configured != "" {
		return "", fmt.Errorf("%w: the configured cloud_token was rejected; create a new one at https://integrations.loqed.com/personal-access-tokens", loqed.ErrUnauthorized)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if m := r.store.Snapshot().Minted; m != nil && m.Value != "" && m.Value != rejected {
		return m.Value, nil
	}
	return r.mintLocked(ctx)
}

func (r *Resolver) mintLocked(ctx context.Context) (string, error) {
	if r.minter == nil {
		return "", ErrNoToken
	}
	if !r.lastMint.IsZero() && r.now().Sub(r.lastMint) < RemintInterval {
		return "", fmt.Errorf("%w: token rejected; next attempt to create one after %s",
			loqed.ErrUnauthorized, r.lastMint.Add(RemintInterval).Format(time.RFC3339))
	}
	r.lastMint = r.now()
	tok, err := r.minter.Mint(ctx)
	if err != nil {
		return "", fmt.Errorf("auth: creating a token with cloud_email/cloud_password failed (set cloud_token to bypass): %w", err)
	}
	if err := r.store.Update(func(c *store.Cache) { c.Minted = &tok }); err != nil {
		r.log.Warn("could not save the new LOQED token", "err", err)
	}
	r.log.Info("created a LOQED personal access token", "token_id", tok.ID)
	return tok.Value, nil
}
```

- [ ] **Step 4: Run tests**

Run: `go test ./internal/auth/ -v -race`
Expected: PASS (6 tests).

- [ ] **Step 5: Commit**

```bash
git add internal/auth
git commit -m "auth: resolve cloud token and mint via portal

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: State model and LOQED → HA mapping

**Files:**
- Create: `internal/model/model.go`, `internal/model/mapping.go`, `internal/model/model_test.go`

**Interfaces:**
- Consumes: `loqed.BoltState`.
- Produces:
  - `type model.Mode string`: `ModeLocal="local"`, `ModeCloud="cloud"`, `ModeOffline="offline"`
  - `type model.LockState string`: `Locked="LOCKED"`, `Unlocked="UNLOCKED"`, `Open="OPEN"`, `Locking="LOCKING"`, `Unlocking="UNLOCKING"`, `Opening="OPENING"`, `Jammed="JAMMED"`
  - `type model.EventType string`: `EventLocked="locked"`, `EventUnlocked="unlocked"`, `EventOpened="opened"`, `EventLocking="locking"`, `EventUnlocking="unlocking"`, `EventOpening="opening"`, `EventJammed="jammed"`, `EventUnknown="unknown"`; `var model.EventTypes []EventType` (that order)
  - `type model.State struct{ Lock *LockState; BoltState loqed.BoltState; BatteryPercentage *int; BatteryVoltage *float64; WifiStrength, BLEStrength *int; LockOnline bool; Mode Mode; LastEvent string; LastKeyID *int; LastKeyName *string; LastEventAt *time.Time; StateStale bool }` (JSON names per spec §6.1 plus `last_key_name`)
  - `type model.Event struct{ EventType EventType; Reason, Source string; KeyLocalID *int; KeyName *string }`
  - `type model.Command string`: `CommandLock="LOCK"`, `CommandUnlock="UNLOCK"`, `CommandOpen="OPEN"`; `func ParseCommand(string) (Command, bool)`; `func (Command) Target() loqed.BoltState`; `func (Command) Moving() LockState`
  - `type model.Transition struct{ SetLock bool; Lock *LockState; SetBolt bool; Bolt loqed.BoltState; Event EventType }`; `func (*State) Apply(Transition)`
  - `func LockStateFor(loqed.BoltState) *LockState` (unknown → nil)
  - `func FromStateReached(eventType string, requested loqed.BoltState) Transition`
  - `func FromGoTo(target loqed.BoltState, current *LockState) Transition`
  - `func Source(eventType string) string`; `func NormalizeKeyID(*int) *int` (nil or 255 → nil); `func Ptr[T any](T) *T`

- [ ] **Step 1: Write failing tests**

`internal/model/model_test.go`:

```go
package model_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/internal/model"
)

func lockStr(l *model.LockState) string {
	if l == nil {
		return "<nil>"
	}
	return string(*l)
}

func TestFromStateReached(t *testing.T) {
	cases := []struct {
		eventType string
		requested loqed.BoltState
		lock      string
		bolt      loqed.BoltState
		event     model.EventType
	}{
		{"STATE_CHANGED_NIGHT_LOCK", loqed.BoltNightLock, "LOCKED", loqed.BoltNightLock, model.EventLocked},
		{"STATE_CHANGED_LATCH", loqed.BoltDayLock, "UNLOCKED", loqed.BoltDayLock, model.EventUnlocked},
		{"STATE_CHANGED_OPEN_REMOTE", loqed.BoltOpen, "OPEN", loqed.BoltOpen, model.EventOpened},
		{"GO_TO_STATE_TOUCH_TO_LOCK", loqed.BoltNightLock, "LOCKED", loqed.BoltNightLock, model.EventLocked},
		// requested_state missing: derive from event type
		{"STATE_CHANGED_NIGHT_LOCK_REMOTE", loqed.BoltUnknown, "LOCKED", loqed.BoltNightLock, model.EventLocked},
		{"STATE_CHANGED_UNKNOWN", loqed.BoltUnknown, "<nil>", loqed.BoltUnknown, model.EventUnknown},
	}
	for _, c := range cases {
		tr := model.FromStateReached(c.eventType, c.requested)
		if !tr.SetLock || lockStr(tr.Lock) != c.lock || !tr.SetBolt || tr.Bolt != c.bolt || tr.Event != c.event {
			t.Errorf("%s: %+v (lock %s)", c.eventType, tr, lockStr(tr.Lock))
		}
	}
	stall := model.FromStateReached("MOTOR_STALL", loqed.BoltUnknown)
	if !stall.SetLock || lockStr(stall.Lock) != "JAMMED" || stall.SetBolt || stall.Event != model.EventJammed {
		t.Errorf("motor stall: %+v", stall)
	}
}

func TestFromGoTo(t *testing.T) {
	locked := model.Locked
	tr := model.FromGoTo(loqed.BoltNightLock, nil)
	if !tr.SetLock || lockStr(tr.Lock) != "LOCKING" || tr.Event != model.EventLocking {
		t.Errorf("%+v", tr)
	}
	tr = model.FromGoTo(loqed.BoltNightLock, &locked)
	if tr.SetLock || tr.Event != model.EventLocking {
		t.Errorf("already locked must not flip to LOCKING: %+v", tr)
	}
	if tr := model.FromGoTo(loqed.BoltDayLock, &locked); lockStr(tr.Lock) != "UNLOCKING" || tr.Event != model.EventUnlocking {
		t.Errorf("%+v", tr)
	}
	if tr := model.FromGoTo(loqed.BoltOpen, &locked); lockStr(tr.Lock) != "OPENING" || tr.Event != model.EventOpening {
		t.Errorf("%+v", tr)
	}
	if tr := model.FromGoTo(loqed.BoltUnknown, &locked); tr.SetLock || tr.Event != model.EventUnknown {
		t.Errorf("%+v", tr)
	}
}

func TestApply(t *testing.T) {
	s := model.State{BoltState: loqed.BoltDayLock, Lock: model.LockStateFor(loqed.BoltDayLock)}
	s.Apply(model.FromGoTo(loqed.BoltNightLock, s.Lock))
	if lockStr(s.Lock) != "LOCKING" || s.BoltState != loqed.BoltDayLock {
		t.Fatalf("%+v", s)
	}
	s.Apply(model.FromStateReached("STATE_CHANGED_UNKNOWN", loqed.BoltUnknown))
	if s.Lock != nil || s.BoltState != loqed.BoltUnknown {
		t.Fatalf("%+v", s)
	}
}

func TestSource(t *testing.T) {
	cases := map[string]string{
		"GO_TO_STATE_TWIST_ASSIST_LATCH":     "twist_assist",
		"GO_TO_STATE_INSTANTOPEN_OPEN":       "instant_open",
		"GO_TO_STATE_TOUCH_TO_LOCK":          "touch",
		"GO_TO_STATE_MANUAL_UNLOCK_BLE_OPEN": "manual",
		"STATE_CHANGED_LATCH_REMOTE":         "remote",
		"GO_TO_STATE_BLE_LATCH":              "ble",
		"STATE_CHANGED_NIGHT_LOCK":           "other",
		"go_to_state_touch_to_lock":          "touch",
	}
	for in, want := range cases {
		if got := model.Source(in); got != want {
			t.Errorf("%s: got %s want %s", in, got, want)
		}
	}
}

func TestNormalizeKeyID(t *testing.T) {
	if model.NormalizeKeyID(nil) != nil || model.NormalizeKeyID(model.Ptr(255)) != nil {
		t.Fatal("nil and 255 must normalize to nil")
	}
	if *model.NormalizeKeyID(model.Ptr(3)) != 3 {
		t.Fatal("3 stays 3")
	}
}

func TestParseCommand(t *testing.T) {
	for in, want := range map[string]model.Command{"LOCK": model.CommandLock, " unlock\n": model.CommandUnlock, "Open": model.CommandOpen} {
		got, ok := model.ParseCommand(in)
		if !ok || got != want {
			t.Errorf("%q: %v %v", in, got, ok)
		}
	}
	if _, ok := model.ParseCommand("RESET"); ok {
		t.Error("RESET must be rejected")
	}
	if model.CommandLock.Target() != loqed.BoltNightLock || model.CommandUnlock.Target() != loqed.BoltDayLock || model.CommandOpen.Target() != loqed.BoltOpen {
		t.Error("targets")
	}
	if model.CommandLock.Moving() != model.Locking || model.CommandOpen.Moving() != model.Opening {
		t.Error("moving states")
	}
}

func TestStateJSON(t *testing.T) {
	at := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	s := model.State{Lock: model.Ptr(model.Locked), BoltState: loqed.BoltNightLock, BatteryPercentage: model.Ptr(78),
		LockOnline: true, Mode: model.ModeLocal, LastEvent: "GO_TO_STATE_TOUCH_TO_LOCK", LastEventAt: &at}
	b, _ := json.Marshal(s)
	for _, want := range []string{`"lock":"LOCKED"`, `"bolt_state":"night_lock"`, `"battery_percentage":78`, `"mode":"local"`,
		`"last_key_id":null`, `"last_event_at":"2026-10-04T12:00:00Z"`, `"state_stale":false`} {
		if !strings.Contains(string(b), want) {
			t.Errorf("missing %s in %s", want, b)
		}
	}
	b, _ = json.Marshal(model.State{})
	if !strings.Contains(string(b), `"lock":null`) {
		t.Errorf("unknown lock must be null: %s", b)
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/model/`
Expected: FAIL — no non-test files.

- [ ] **Step 3: Implement `model.go`**

```go
// Package model holds the per-lock state document published to MQTT and
// the mapping from LOQED events to Home Assistant states and events.
package model

import (
	"strings"
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
)

type Mode string

const (
	ModeLocal   Mode = "local"
	ModeCloud   Mode = "cloud"
	ModeOffline Mode = "offline"
)

// LockState values match the HA MQTT lock's state_* payloads.
type LockState string

const (
	Locked    LockState = "LOCKED"
	Unlocked  LockState = "UNLOCKED"
	Open      LockState = "OPEN"
	Locking   LockState = "LOCKING"
	Unlocking LockState = "UNLOCKING"
	Opening   LockState = "OPENING"
	Jammed    LockState = "JAMMED"
)

// EventType values are the HA event entity's event_types.
type EventType string

const (
	EventLocked    EventType = "locked"
	EventUnlocked  EventType = "unlocked"
	EventOpened    EventType = "opened"
	EventLocking   EventType = "locking"
	EventUnlocking EventType = "unlocking"
	EventOpening   EventType = "opening"
	EventJammed    EventType = "jammed"
	EventUnknown   EventType = "unknown"
)

var EventTypes = []EventType{EventLocked, EventUnlocked, EventOpened, EventLocking, EventUnlocking, EventOpening, EventJammed, EventUnknown}

// State is the retained JSON document on <base>/<id>/state.
type State struct {
	Lock              *LockState      `json:"lock"`
	BoltState         loqed.BoltState `json:"bolt_state"`
	BatteryPercentage *int            `json:"battery_percentage"`
	BatteryVoltage    *float64        `json:"battery_voltage"`
	WifiStrength      *int            `json:"wifi_strength"`
	BLEStrength       *int            `json:"ble_strength"`
	LockOnline        bool            `json:"lock_online"`
	Mode              Mode            `json:"mode"`
	LastEvent         string          `json:"last_event"`
	LastKeyID         *int            `json:"last_key_id"`
	LastKeyName       *string         `json:"last_key_name"`
	LastEventAt       *time.Time      `json:"last_event_at"`
	StateStale        bool            `json:"state_stale"`
}

// Event is the non-retained JSON on <base>/<id>/event.
type Event struct {
	EventType  EventType `json:"event_type"`
	Reason     string    `json:"reason"`
	Source     string    `json:"source"`
	KeyLocalID *int      `json:"key_local_id"`
	KeyName    *string   `json:"key_name"`
}

type Command string

const (
	CommandLock   Command = "LOCK"
	CommandUnlock Command = "UNLOCK"
	CommandOpen   Command = "OPEN"
)

func ParseCommand(s string) (Command, bool) {
	switch c := Command(strings.ToUpper(strings.TrimSpace(s))); c {
	case CommandLock, CommandUnlock, CommandOpen:
		return c, true
	default:
		return "", false
	}
}

func (c Command) Target() loqed.BoltState {
	switch c {
	case CommandOpen:
		return loqed.BoltOpen
	case CommandUnlock:
		return loqed.BoltDayLock
	default:
		return loqed.BoltNightLock
	}
}

func (c Command) Moving() LockState {
	switch c {
	case CommandOpen:
		return Opening
	case CommandUnlock:
		return Unlocking
	default:
		return Locking
	}
}

func Ptr[T any](v T) *T { return &v }
```

- [ ] **Step 4: Implement `mapping.go`**

```go
package model

import (
	"strings"

	loqed "github.com/t3hk0d3/go-loqed"
)

// Transition is the effect of one LOQED event on State.
type Transition struct {
	SetLock bool
	Lock    *LockState // nil with SetLock = unknown
	SetBolt bool
	Bolt    loqed.BoltState
	Event   EventType
}

func (s *State) Apply(t Transition) {
	if t.SetLock {
		s.Lock = t.Lock
	}
	if t.SetBolt {
		s.BoltState = t.Bolt
	}
}

// LockStateFor maps a bolt position; unknown maps to nil (HA "unknown",
// matching HA core since 2026-10-01).
func LockStateFor(b loqed.BoltState) *LockState {
	switch b {
	case loqed.BoltNightLock:
		return Ptr(Locked)
	case loqed.BoltDayLock:
		return Ptr(Unlocked)
	case loqed.BoltOpen:
		return Ptr(Open)
	default:
		return nil
	}
}

// FromStateReached handles STATE_CHANGED_* (incl. *_REMOTE), MOTOR_STALL and
// other "state reached" events. requested may be BoltUnknown when absent.
func FromStateReached(eventType string, requested loqed.BoltState) Transition {
	et := strings.ToUpper(strings.TrimSpace(eventType))
	if et == "MOTOR_STALL" {
		return Transition{SetLock: true, Lock: Ptr(Jammed), Event: EventJammed}
	}
	b := requested
	if b == "" || b == loqed.BoltUnknown {
		b = boltFromEventType(et)
	}
	t := Transition{SetLock: true, Lock: LockStateFor(b), SetBolt: true, Bolt: b}
	switch b {
	case loqed.BoltNightLock:
		t.Event = EventLocked
	case loqed.BoltDayLock:
		t.Event = EventUnlocked
	case loqed.BoltOpen:
		t.Event = EventOpened
	default:
		t.Bolt, t.Event = loqed.BoltUnknown, EventUnknown
	}
	return t
}

func boltFromEventType(et string) loqed.BoltState {
	switch strings.TrimSuffix(et, "_REMOTE") {
	case "STATE_CHANGED_OPEN":
		return loqed.BoltOpen
	case "STATE_CHANGED_LATCH":
		return loqed.BoltDayLock
	case "STATE_CHANGED_NIGHT_LOCK":
		return loqed.BoltNightLock
	default:
		return loqed.BoltUnknown
	}
}

// FromGoTo handles GO_TO_STATE_* events. The lock only shows a moving
// state when the target differs from the current state.
func FromGoTo(target loqed.BoltState, current *LockState) Transition {
	var want, moving LockState
	var ev EventType
	switch target {
	case loqed.BoltNightLock:
		want, moving, ev = Locked, Locking, EventLocking
	case loqed.BoltDayLock:
		want, moving, ev = Unlocked, Unlocking, EventUnlocking
	case loqed.BoltOpen:
		want, moving, ev = Open, Opening, EventOpening
	default:
		return Transition{Event: EventUnknown}
	}
	t := Transition{Event: ev}
	if current == nil || *current != want {
		t.SetLock, t.Lock = true, Ptr(moving)
	}
	return t
}

var sources = []struct{ token, source string }{
	{"TWIST_ASSIST", "twist_assist"},
	{"INSTANTOPEN", "instant_open"},
	{"TOUCH", "touch"},
	{"MANUAL", "manual"},
	{"REMOTE", "remote"},
	{"BLE", "ble"},
}

// Source classifies how the lock was operated, from the raw event type.
func Source(eventType string) string {
	et := strings.ToUpper(eventType)
	for _, s := range sources {
		if strings.Contains(et, s.token) {
			return s.source
		}
	}
	return "other"
}

// NormalizeKeyID drops absent and 255 ("unknown key") ids.
func NormalizeKeyID(id *int) *int {
	if id == nil || *id == 255 {
		return nil
	}
	v := *id
	return &v
}
```

- [ ] **Step 5: Run tests**

Run: `go test ./internal/model/ -v`
Expected: PASS (8 tests).

- [ ] **Step 6: Commit**

```bash
git add internal/model
git commit -m "model: add state document and LOQED to HA mapping

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: MQTT topics and HA discovery payload

**Files:**
- Create: `internal/hass/topics.go`, `internal/hass/discovery.go`, `internal/hass/discovery_test.go`, `internal/hass/testdata/discovery_lock1.golden.json` (generated)

**Interfaces:**
- Consumes: `model.EventTypes`.
- Produces:
  - `type hass.Topics struct{ Base, DiscoveryPrefix string }` with `Status()`, `Availability(id)`, `State(id)`, `Event(id)`, `Command(id)`, `CommandWildcard()`, `Discovery(id)`, `HAStatus()` (all `string`; `id` is already a topic id)
  - `func hass.TopicID(lockID string) string` — keeps `[A-Za-z0-9_-]`, replaces anything else with `_`
  - `type hass.LockInfo struct{ ID, Name, Model, MacWifi string }`
  - `func hass.DiscoveryPayload(t Topics, l LockInfo, version string) ([]byte, error)`

- [ ] **Step 1: Write failing tests**

`internal/hass/discovery_test.go`:

```go
package hass_test

import (
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/t3hk0d3/go-loqed/internal/hass"
)

var update = flag.Bool("update", false, "rewrite golden files")

var topics = hass.Topics{Base: "loqed", DiscoveryPrefix: "homeassistant"}

func TestTopics(t *testing.T) {
	cases := map[string]string{
		topics.Status():            "loqed/status",
		topics.Availability("abc"): "loqed/abc/availability",
		topics.State("abc"):        "loqed/abc/state",
		topics.Event("abc"):        "loqed/abc/event",
		topics.Command("abc"):      "loqed/abc/command",
		topics.CommandWildcard():   "loqed/+/command",
		topics.Discovery("abc"):    "homeassistant/device/loqed_abc/config",
		topics.HAStatus():          "homeassistant/status",
	}
	for got, want := range cases {
		if got != want {
			t.Errorf("got %s want %s", got, want)
		}
	}
	if hass.TopicID("Yq1g/K4+#x y") != "Yq1g_K4___x_y" {
		t.Errorf("TopicID: %s", hass.TopicID("Yq1g/K4+#x y"))
	}
}

func TestDiscoveryPayload(t *testing.T) {
	b, err := hass.DiscoveryPayload(topics, hass.LockInfo{ID: "lock1", Name: "Front door", Model: "LOQED Touch", MacWifi: "AA:BB:CC:DD:EE:FF"}, "1.2.3")
	if err != nil {
		t.Fatal(err)
	}
	var p struct {
		Device struct {
			Identifiers  []string   `json:"identifiers"`
			Name         string     `json:"name"`
			Manufacturer string     `json:"manufacturer"`
			Connections  [][]string `json:"connections"`
		} `json:"device"`
		Availability     []map[string]string       `json:"availability"`
		AvailabilityMode string                    `json:"availability_mode"`
		Components       map[string]map[string]any `json:"components"`
	}
	if err := json.Unmarshal(b, &p); err != nil {
		t.Fatal(err)
	}
	if p.Device.Identifiers[0] != "loqed_lock1" || p.Device.Name != "Front door" || p.Device.Manufacturer != "LOQED" ||
		p.Device.Connections[0][1] != "aa:bb:cc:dd:ee:ff" {
		t.Fatalf("device %+v", p.Device)
	}
	if p.AvailabilityMode != "all" || p.Availability[0]["topic"] != "loqed/status" || p.Availability[1]["topic"] != "loqed/lock1/availability" {
		t.Fatalf("availability %+v %s", p.Availability, p.AvailabilityMode)
	}
	wantPlatforms := map[string]string{"lock": "lock", "battery": "sensor", "battery_voltage": "sensor", "wifi_signal": "sensor",
		"ble_signal": "sensor", "lock_online": "binary_sensor", "connection_mode": "sensor", "last_change_reason": "sensor", "event": "event"}
	if len(p.Components) != len(wantPlatforms) {
		t.Fatalf("components %v", p.Components)
	}
	for id, platform := range wantPlatforms {
		c := p.Components[id]
		if c["platform"] != platform || c["unique_id"] != "loqed_lock1_"+id {
			t.Errorf("%s: %v", id, c)
		}
	}
	if p.Components["lock"]["command_topic"] != "loqed/lock1/command" || p.Components["event"]["state_topic"] != "loqed/lock1/event" {
		t.Errorf("topics wrong")
	}
	if p.Components["lock"]["retain"] != nil {
		t.Errorf("commands must not be retained")
	}

	golden := filepath.Join("testdata", "discovery_lock1.golden.json")
	var pretty bytes.Buffer
	_ = json.Indent(&pretty, b, "", "  ")
	if *update {
		_ = os.MkdirAll("testdata", 0o755)
		if err := os.WriteFile(golden, pretty.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatalf("missing golden file; run go test ./internal/hass -run TestDiscoveryPayload -update: %v", err)
	}
	if !bytes.Equal(want, pretty.Bytes()) {
		t.Fatalf("discovery payload changed; review and run with -update.\n got: %s", pretty.Bytes())
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/hass/`
Expected: FAIL — no non-test files.

- [ ] **Step 3: Implement `topics.go`**

```go
// Package hass publishes lock state to MQTT and Home Assistant discovery.
package hass

import "strings"

type Topics struct {
	Base            string
	DiscoveryPrefix string
}

func (t Topics) Status() string                { return t.Base + "/status" }
func (t Topics) Availability(id string) string { return t.Base + "/" + id + "/availability" }
func (t Topics) State(id string) string        { return t.Base + "/" + id + "/state" }
func (t Topics) Event(id string) string        { return t.Base + "/" + id + "/event" }
func (t Topics) Command(id string) string      { return t.Base + "/" + id + "/command" }
func (t Topics) CommandWildcard() string       { return t.Base + "/+/command" }
func (t Topics) Discovery(id string) string    { return t.DiscoveryPrefix + "/device/loqed_" + id + "/config" }
func (t Topics) HAStatus() string              { return t.DiscoveryPrefix + "/status" }

// TopicID makes a lock id safe for use as one MQTT topic level.
func TopicID(lockID string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_', r == '-':
			return r
		default:
			return '_'
		}
	}, lockID)
}
```

- [ ] **Step 4: Implement `discovery.go`**

HA renders a JSON `null` as `None`, which MQTT lock and sensor entities treat as "unknown", so plain `{{ value_json.x }}` templates are enough.

```go
package hass

import (
	"encoding/json"
	"strings"

	"github.com/t3hk0d3/go-loqed/internal/model"
)

type LockInfo struct {
	ID      string
	Name    string
	Model   string
	MacWifi string
}

// DiscoveryPayload builds the device-based discovery message: one device
// per lock with all of its entities as components.
func DiscoveryPayload(t Topics, l LockInfo, version string) ([]byte, error) {
	tid := TopicID(l.ID)
	uid := "loqed_" + tid
	state := t.State(tid)
	modelName := l.Model
	if modelName == "" {
		modelName = "Smart lock"
	}
	device := map[string]any{"identifiers": []string{uid}, "name": l.Name, "manufacturer": "LOQED", "model": modelName}
	if l.MacWifi != "" {
		device["connections"] = [][]string{{"mac", strings.ToLower(l.MacWifi)}}
	}
	sensor := func(id, name, field string, extra map[string]any) map[string]any {
		c := map[string]any{"platform": "sensor", "name": name, "unique_id": uid + "_" + id, "state_topic": state,
			"value_template": "{{ value_json." + field + " }}"}
		for k, v := range extra {
			c[k] = v
		}
		return c
	}
	eventTypes := make([]string, len(model.EventTypes))
	for i, e := range model.EventTypes {
		eventTypes[i] = string(e)
	}
	components := map[string]any{
		"lock": map[string]any{
			"platform": "lock", "name": nil, "unique_id": uid + "_lock",
			"state_topic": state, "value_template": "{{ value_json.lock }}",
			"command_topic": t.Command(tid), "qos": 1,
			"payload_lock": string(model.CommandLock), "payload_unlock": string(model.CommandUnlock), "payload_open": string(model.CommandOpen),
			"state_locked": string(model.Locked), "state_unlocked": string(model.Unlocked), "state_open": string(model.Open),
			"state_locking": string(model.Locking), "state_unlocking": string(model.Unlocking), "state_opening": string(model.Opening),
			"state_jammed": string(model.Jammed),
		},
		"battery": sensor("battery", "Battery", "battery_percentage",
			map[string]any{"device_class": "battery", "unit_of_measurement": "%", "state_class": "measurement"}),
		"battery_voltage": sensor("battery_voltage", "Battery voltage", "battery_voltage",
			map[string]any{"device_class": "voltage", "unit_of_measurement": "V", "state_class": "measurement", "entity_category": "diagnostic"}),
		"wifi_signal": sensor("wifi_signal", "Wi-Fi signal", "wifi_strength",
			map[string]any{"state_class": "measurement", "entity_category": "diagnostic"}),
		"ble_signal": sensor("ble_signal", "Bluetooth signal", "ble_strength",
			map[string]any{"state_class": "measurement", "entity_category": "diagnostic"}),
		"lock_online": map[string]any{
			"platform": "binary_sensor", "name": "Lock online", "unique_id": uid + "_lock_online", "state_topic": state,
			"value_template": "{{ 'ON' if value_json.lock_online else 'OFF' }}", "device_class": "connectivity", "entity_category": "diagnostic",
		},
		"connection_mode": sensor("connection_mode", "Connection mode", "mode",
			map[string]any{"device_class": "enum", "options": []string{"local", "cloud", "offline"}, "entity_category": "diagnostic"}),
		"last_change_reason": sensor("last_change_reason", "Last change reason", "last_event", map[string]any{
			"json_attributes_topic": state,
			"json_attributes_template": "{{ {'key_local_id': value_json.last_key_id, 'key_name': value_json.last_key_name, " +
				"'last_event_at': value_json.last_event_at} | tojson }}",
		}),
		"event": map[string]any{
			"platform": "event", "name": "Lock event", "unique_id": uid + "_event", "state_topic": t.Event(tid), "event_types": eventTypes,
		},
	}
	return json.Marshal(map[string]any{
		"device":            device,
		"origin":            map[string]any{"name": "loqed-mqtt", "sw_version": version, "support_url": "https://github.com/t3hk0d3/go-loqed"},
		"components":        components,
		"availability":      []map[string]string{{"topic": t.Status()}, {"topic": t.Availability(tid)}},
		"availability_mode": "all",
		"qos":               1,
	})
}
```

- [ ] **Step 5: Generate and review the golden file**

Run: `go test ./internal/hass/ -run TestDiscoveryPayload -update && go test ./internal/hass/ -v`
Expected: PASS. Open `internal/hass/testdata/discovery_lock1.golden.json` and check by eye: 9 components, lock `"name": null`, `"availability_mode": "all"`, event `event_types` lists the 8 normalized types.

- [ ] **Step 6: Commit**

```bash
git add internal/hass
git commit -m "hass: add topic layout and device discovery payload

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: MQTT client

**Files:**
- Create: `internal/testutil/mqtt.go`, `internal/hass/client.go`, `internal/hass/client_test.go`

**Interfaces:**
- Consumes: `Topics`, `TopicID`, `LockInfo`, `DiscoveryPayload`, `model.State`, `model.Event`, `model.ParseCommand`.
- Produces:
  - `type hass.ClientConfig struct{ URL, Username, Password, ClientID string; Topics Topics; HAEnabled bool; Version string }`
  - `type hass.Command struct{ LockID string; Command model.Command; At time.Time }` (LockID is the real cloud id)
  - `func hass.NewClient(cfg ClientConfig, log *slog.Logger) *Client`
  - `(*Client).Start()` (connects in the background, retrying forever), `Close()`, `Connected() bool`, `Commands() <-chan Command`
  - `(*Client).SetLocks(locks []LockInfo, removed []string)` (`removed` are real lock ids whose discovery must be cleared)
  - `(*Client).PublishState(lockID string, s model.State) error`, `PublishEvent(lockID string, e model.Event) error`, `PublishAvailability(lockID string, online bool) error` (dedupes unchanged availability). While disconnected these return nil: state and availability are cached and republished on connect; events are dropped.
  - Test helpers: `testutil.StartBroker(t) string` (returns `tcp://127.0.0.1:port`), `testutil.Subscribe(t, url, filter string) *Subscriber`, `(*Subscriber).WaitFor(t, timeout, func(Message) bool) Message`, `(*Subscriber).Count(func(Message) bool) int`, `(*Subscriber).Publish(t, topic, payload string, retained bool)`, `type testutil.Message struct{ Topic string; Payload []byte; Retained bool }`

- [ ] **Step 1: Add dependencies**

Run: `go get github.com/eclipse/paho.mqtt.golang@latest github.com/mochi-mqtt/server/v2@latest`

- [ ] **Step 2: Write the test helpers**

`internal/testutil/mqtt.go` (only imported from tests):

```go
// Package testutil provides an in-process MQTT broker and subscriber for tests.
package testutil

import (
	"net"
	"sync"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	mqttserver "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"
)

// StartBroker runs a mochi broker on a free localhost port.
func StartBroker(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	srv := mqttserver.New(nil)
	if err := srv.AddHook(new(auth.AllowHook), nil); err != nil {
		t.Fatal(err)
	}
	// mochi v2.6+: NewTCP(listeners.Config). If the signature differs, check `go doc github.com/mochi-mqtt/server/v2/listeners NewTCP`.
	if err := srv.AddListener(listeners.NewTCP(listeners.Config{ID: "test", Address: addr})); err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve() }()
	t.Cleanup(func() { _ = srv.Close() })
	return "tcp://" + addr
}

type Message struct {
	Topic    string
	Payload  []byte
	Retained bool
}

type Subscriber struct {
	mu   sync.Mutex
	msgs []Message
	c    mqtt.Client
}

func Subscribe(t *testing.T, url, filter string) *Subscriber {
	t.Helper()
	s := &Subscriber{}
	opts := mqtt.NewClientOptions().AddBroker(url).SetClientID("sub-" + t.Name()).SetCleanSession(true)
	s.c = mqtt.NewClient(opts)
	if tok := s.c.Connect(); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("subscriber connect: %v", tok.Error())
	}
	tok := s.c.Subscribe(filter, 1, func(_ mqtt.Client, m mqtt.Message) {
		s.mu.Lock()
		s.msgs = append(s.msgs, Message{Topic: m.Topic(), Payload: append([]byte(nil), m.Payload()...), Retained: m.Retained()})
		s.mu.Unlock()
	})
	if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("subscribe: %v", tok.Error())
	}
	t.Cleanup(func() { s.c.Disconnect(100) })
	return s
}

func (s *Subscriber) WaitFor(t *testing.T, timeout time.Duration, match func(Message) bool) Message {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		for _, m := range s.msgs {
			if match(m) {
				s.mu.Unlock()
				return m
			}
		}
		s.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var topics []string
	for _, m := range s.msgs {
		topics = append(topics, m.Topic+"="+string(m.Payload))
	}
	t.Fatalf("no matching MQTT message within %s; got %v", timeout, topics)
	return Message{}
}

func (s *Subscriber) Count(match func(Message) bool) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, m := range s.msgs {
		if match(m) {
			n++
		}
	}
	return n
}

func (s *Subscriber) Publish(t *testing.T, topic, payload string, retained bool) {
	t.Helper()
	if tok := s.c.Publish(topic, 1, retained, payload); !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		t.Fatalf("publish: %v", tok.Error())
	}
}

// Topic returns a matcher for messages on topic.
func Topic(topic string) func(Message) bool {
	return func(m Message) bool { return m.Topic == topic }
}
```

- [ ] **Step 3: Write failing client tests**

`internal/hass/client_test.go`:

```go
package hass_test

import (
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/t3hk0d3/go-loqed/internal/hass"
	"github.com/t3hk0d3/go-loqed/internal/model"
	"github.com/t3hk0d3/go-loqed/internal/testutil"
)

const wait = 5 * time.Second

func startClient(t *testing.T, url string, haEnabled bool) *hass.Client {
	t.Helper()
	c := hass.NewClient(hass.ClientConfig{URL: url, ClientID: "gw-" + t.Name(), Topics: topics, HAEnabled: haEnabled, Version: "test"},
		slog.New(slog.DiscardHandler))
	c.SetLocks([]hass.LockInfo{{ID: "lock1", Name: "Front door"}}, []string{"gone"})
	c.Start()
	t.Cleanup(c.Close)
	deadline := time.Now().Add(wait)
	for !c.Connected() {
		if time.Now().After(deadline) {
			t.Fatal("client did not connect")
		}
		time.Sleep(20 * time.Millisecond)
	}
	return c
}

func TestPublishesStatusDiscoveryAndRetainedState(t *testing.T) {
	url := testutil.StartBroker(t)
	c := startClient(t, url, true)
	if err := c.PublishState("lock1", model.State{Lock: model.Ptr(model.Locked), Mode: model.ModeLocal}); err != nil {
		t.Fatal(err)
	}
	if err := c.PublishAvailability("lock1", true); err != nil {
		t.Fatal(err)
	}
	// A subscriber connecting later must see retained messages.
	sub := testutil.Subscribe(t, url, "#")
	sub.WaitFor(t, wait, func(m testutil.Message) bool { return m.Topic == "loqed/status" && string(m.Payload) == "online" && m.Retained })
	sub.WaitFor(t, wait, func(m testutil.Message) bool {
		return m.Topic == "homeassistant/device/loqed_lock1/config" && m.Retained && len(m.Payload) > 0
	})
	sub.WaitFor(t, wait, func(m testutil.Message) bool { return m.Topic == "loqed/lock1/availability" && string(m.Payload) == "online" })
	st := sub.WaitFor(t, wait, testutil.Topic("loqed/lock1/state"))
	var s model.State
	if err := json.Unmarshal(st.Payload, &s); err != nil || s.Lock == nil || *s.Lock != model.Locked {
		t.Fatalf("state %s %v", st.Payload, err)
	}
}

func TestRemovedLockDiscoveryIsCleared(t *testing.T) {
	url := testutil.StartBroker(t)
	sub := testutil.Subscribe(t, url, "homeassistant/#")
	startClient(t, url, true)
	sub.WaitFor(t, wait, func(m testutil.Message) bool {
		return m.Topic == "homeassistant/device/loqed_gone/config" && len(m.Payload) == 0
	})
}

func TestEventsAreNotRetained(t *testing.T) {
	url := testutil.StartBroker(t)
	sub := testutil.Subscribe(t, url, "loqed/#")
	c := startClient(t, url, true)
	if err := c.PublishEvent("lock1", model.Event{EventType: model.EventLocked, Reason: "STATE_CHANGED_NIGHT_LOCK"}); err != nil {
		t.Fatal(err)
	}
	m := sub.WaitFor(t, wait, testutil.Topic("loqed/lock1/event"))
	if m.Retained {
		t.Fatal("event must not be retained")
	}
	late := testutil.Subscribe(t, url, "loqed/lock1/event")
	time.Sleep(300 * time.Millisecond)
	if late.Count(testutil.Topic("loqed/lock1/event")) != 0 {
		t.Fatal("late subscriber must not receive old events")
	}
}

func TestCommandsAreDelivered(t *testing.T) {
	url := testutil.StartBroker(t)
	c := startClient(t, url, true)
	sub := testutil.Subscribe(t, url, "unused/#")
	time.Sleep(200 * time.Millisecond) // let the client's subscription settle
	sub.Publish(t, "loqed/lock1/command", "lock", false)
	sub.Publish(t, "loqed/unknown/command", "LOCK", false)
	sub.Publish(t, "loqed/lock1/command", "EXPLODE", false)
	select {
	case cmd := <-c.Commands():
		if cmd.LockID != "lock1" || cmd.Command != model.CommandLock || cmd.At.IsZero() {
			t.Fatalf("got %+v", cmd)
		}
	case <-time.After(wait):
		t.Fatal("no command")
	}
	select {
	case cmd := <-c.Commands():
		t.Fatalf("unexpected extra command %+v", cmd)
	case <-time.After(300 * time.Millisecond):
	}
}

func TestHABirthRepublishesDiscovery(t *testing.T) {
	url := testutil.StartBroker(t)
	sub := testutil.Subscribe(t, url, "homeassistant/device/#")
	startClient(t, url, true)
	isDiscovery := func(m testutil.Message) bool {
		return m.Topic == "homeassistant/device/loqed_lock1/config" && len(m.Payload) > 0
	}
	sub.WaitFor(t, wait, isDiscovery)
	before := sub.Count(isDiscovery)
	sub.Publish(t, "homeassistant/status", "online", false)
	deadline := time.Now().Add(wait)
	for sub.Count(isDiscovery) <= before {
		if time.Now().After(deadline) {
			t.Fatal("discovery not republished after HA birth message")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestDiscoveryDisabled(t *testing.T) {
	url := testutil.StartBroker(t)
	sub := testutil.Subscribe(t, url, "#")
	c := startClient(t, url, false)
	_ = c.PublishState("lock1", model.State{Mode: model.ModeLocal})
	sub.WaitFor(t, wait, testutil.Topic("loqed/lock1/state"))
	if n := sub.Count(func(m testutil.Message) bool { return len(m.Topic) > 13 && m.Topic[:13] == "homeassistant" }); n != 0 {
		t.Fatalf("discovery published while disabled (%d messages)", n)
	}
}

func TestAvailabilityIsDeduplicated(t *testing.T) {
	url := testutil.StartBroker(t)
	sub := testutil.Subscribe(t, url, "loqed/lock1/availability")
	c := startClient(t, url, true)
	for range 3 {
		_ = c.PublishAvailability("lock1", true)
	}
	sub.WaitFor(t, wait, testutil.Topic("loqed/lock1/availability"))
	time.Sleep(300 * time.Millisecond)
	if n := sub.Count(testutil.Topic("loqed/lock1/availability")); n != 1 {
		t.Fatalf("availability published %d times", n)
	}
}
```

- [ ] **Step 4: Run to verify failure**

Run: `go test ./internal/hass/ -run 'Publish|Removed|Events|Commands|Birth|Disabled|Availability'`
Expected: FAIL — `undefined: hass.NewClient`.

- [ ] **Step 5: Implement the client**

`internal/hass/client.go`:

```go
package hass

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
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
}

// Command is a lock command received over MQTT.
type Command struct {
	LockID  string
	Command model.Command
	At      time.Time
}

type Client struct {
	cfg      ClientConfig
	log      *slog.Logger
	mc       mqtt.Client
	commands chan Command
	now      func() time.Time

	mu      sync.Mutex
	locks   map[string]LockInfo // by topic id
	removed []string            // real lock ids
	states  map[string][]byte   // by real lock id
	avail   map[string]string   // by real lock id
}

const publishTimeout = 5 * time.Second

func NewClient(cfg ClientConfig, log *slog.Logger) *Client {
	c := &Client{cfg: cfg, log: log, commands: make(chan Command, 16), now: time.Now,
		locks: map[string]LockInfo{}, states: map[string][]byte{}, avail: map[string]string{}}
	opts := mqtt.NewClientOptions().
		AddBroker(cfg.URL).
		SetClientID(cfg.ClientID).
		SetUsername(cfg.Username).
		SetPassword(cfg.Password).
		SetCleanSession(true).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetConnectRetryInterval(5 * time.Second).
		SetMaxReconnectInterval(time.Minute).
		SetKeepAlive(30 * time.Second).
		SetOrderMatters(false).
		SetWill(cfg.Topics.Status(), "offline", 1, true).
		SetOnConnectHandler(func(mqtt.Client) { go c.onConnect() }).
		SetConnectionLostHandler(func(_ mqtt.Client, err error) { log.Warn("MQTT connection lost", "err", err) })
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

func (c *Client) Commands() <-chan Command { return c.commands }

func (c *Client) SetLocks(locks []LockInfo, removed []string) {
	c.mu.Lock()
	c.locks = make(map[string]LockInfo, len(locks))
	for _, l := range locks {
		c.locks[TopicID(l.ID)] = l
	}
	c.removed = removed
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
	c.mu.Lock()
	c.states[lockID] = b
	c.mu.Unlock()
	return c.publish(c.cfg.Topics.State(TopicID(lockID)), true, b)
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
	c.log.Info("connected to MQTT broker", "url", c.cfg.URL)
	if err := c.publish(t.Status(), true, []byte("online")); err != nil {
		c.log.Warn("publishing gateway status failed", "err", err)
	}
	c.mc.Subscribe(t.CommandWildcard(), 1, c.onCommand)
	if c.cfg.HAEnabled {
		c.mc.Subscribe(t.HAStatus(), 1, func(_ mqtt.Client, m mqtt.Message) {
			if string(m.Payload()) == "online" {
				c.log.Info("Home Assistant came online; republishing discovery")
				go c.publishDiscovery()
			}
		})
	}
	c.publishDiscovery()
	c.mu.Lock()
	states, avail := maps.Clone(c.states), maps.Clone(c.avail)
	c.mu.Unlock()
	for id, v := range avail {
		_ = c.publish(t.Availability(TopicID(id)), true, []byte(v))
	}
	for id, b := range states {
		_ = c.publish(t.State(TopicID(id)), true, b)
	}
}

func (c *Client) publishDiscovery() {
	if !c.cfg.HAEnabled {
		return
	}
	t := c.cfg.Topics
	c.mu.Lock()
	locks := make([]LockInfo, 0, len(c.locks))
	for _, l := range c.locks {
		locks = append(locks, l)
	}
	removed := append([]string(nil), c.removed...)
	c.mu.Unlock()
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
	for _, id := range removed {
		_ = c.publish(t.Discovery(TopicID(id)), true, []byte{})
	}
}

func (c *Client) onCommand(_ mqtt.Client, m mqtt.Message) {
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
	cmd, ok := model.ParseCommand(string(m.Payload()))
	if !ok {
		c.log.Warn("unknown command ignored; use LOCK, UNLOCK or OPEN", "topic", m.Topic())
		return
	}
	select {
	case c.commands <- Command{LockID: l.ID, Command: cmd, At: c.now()}:
	default:
		c.log.Warn("command queue full; command dropped", "lock_id", l.ID, "command", cmd)
	}
}
```

- [ ] **Step 6: Run tests**

Run: `go test ./internal/hass/ -v -race`
Expected: PASS. Broker tests take a few seconds.

- [ ] **Step 7: Commit**

```bash
git add go.mod go.sum internal/hass internal/testutil
git commit -m "hass: add MQTT client with discovery, commands and birth handling

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: Cloud request budget and cloud hub

**Files:**
- Create: `internal/gateway/budget.go`, `internal/gateway/cloudhub.go`, `internal/gateway/budget_test.go`, `internal/gateway/cloudhub_test.go`

**Interfaces:**
- Consumes: `cloud.Lock`, `loqed.ErrUnauthorized`, `loqed.ErrRateLimited`, `loqed.BoltState`.
- Produces:
  - `type gateway.Priority int`: `PriorityRefresh`, `PriorityConfirm`, `PriorityBackground`
  - `var gateway.ErrBudgetExhausted, ErrDeferred, ErrCloudBlocked`
  - `func gateway.NewBudget(limit int, window time.Duration, now func() time.Time) *Budget`; `(*Budget).Take(Priority) error`, `Block(time.Duration)`, `Remaining() int`
  - `type gateway.CloudAPI interface{ ListLocks(ctx) ([]cloud.Lock, error); Command(ctx, lockID string, s loqed.BoltState) error }` (satisfied by `*cloud.Client`)
  - `type gateway.TokenSource interface{ Token(ctx) (string, error); Invalidate(ctx, rejected string) (string, error) }` (satisfied by `*auth.Resolver`)
  - `const gateway.RateLimitBackoff = 12 * time.Hour`
  - `func gateway.NewCloudHub(b *Budget, tokens TokenSource, newAPI func(token string) CloudAPI, now func() time.Time, log *slog.Logger) *CloudHub`; `(*CloudHub).Locks(ctx, Priority) ([]cloud.Lock, error)`, `Command(ctx, lockID string, s loqed.BoltState) error`, `Token() string`

Budget rules (spec §5.6): rolling window; background polls keep `min(2, limit-1)` calls in reserve and are spaced at least `window/limit` apart (`ErrDeferred`, not stale); `ErrBudgetExhausted` means stale. Hub: one `ListLocks` serves all locks for 30 s; on `ErrUnauthorized` it asks the token source for a replacement and retries once; on `ErrRateLimited` it blocks the budget for 12 h. Command calls are not budgeted (spec V2).

- [ ] **Step 1: Write failing tests**

`internal/gateway/budget_test.go`:

```go
package gateway

import (
	"errors"
	"testing"
	"time"
)

func TestBudgetLimitsCallsPerWindow(t *testing.T) {
	now := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	b := NewBudget(3, 12*time.Hour, func() time.Time { return now })
	for i := range 3 {
		if err := b.Take(PriorityRefresh); err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
	}
	if err := b.Take(PriorityConfirm); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("got %v", err)
	}
	now = now.Add(12 * time.Hour)
	if err := b.Take(PriorityConfirm); err != nil {
		t.Fatalf("window should have rolled: %v", err)
	}
}

func TestBudgetBackgroundReserveAndSpacing(t *testing.T) {
	now := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	b := NewBudget(10, 12*time.Hour, func() time.Time { return now })
	if err := b.Take(PriorityBackground); err != nil {
		t.Fatal(err)
	}
	if err := b.Take(PriorityBackground); !errors.Is(err, ErrDeferred) {
		t.Fatalf("second background poll within 72m must be deferred: %v", err)
	}
	for i := range 7 {
		now = now.Add(72 * time.Minute)
		if err := b.Take(PriorityBackground); err != nil {
			t.Fatalf("background %d: %v", i, err)
		}
	}
	now = now.Add(72 * time.Minute)
	// 8 used, 2 left = reserve: background refused, confirm allowed.
	if b.Remaining() != 2 {
		t.Fatalf("remaining %d", b.Remaining())
	}
	if err := b.Take(PriorityBackground); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("got %v", err)
	}
	if err := b.Take(PriorityConfirm); err != nil {
		t.Fatal(err)
	}
}

func TestBudgetBlock(t *testing.T) {
	now := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	b := NewBudget(10, 12*time.Hour, func() time.Time { return now })
	b.Block(12 * time.Hour)
	if err := b.Take(PriorityRefresh); !errors.Is(err, ErrCloudBlocked) {
		t.Fatalf("got %v", err)
	}
	now = now.Add(12 * time.Hour)
	if err := b.Take(PriorityRefresh); err != nil {
		t.Fatal(err)
	}
}
```

`internal/gateway/cloudhub_test.go`:

```go
package gateway

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/cloud"
)

type scriptedAPI struct {
	token    string
	errs     []error // consumed per ListLocks call
	calls    int
	commands int
}

func (a *scriptedAPI) ListLocks(context.Context) ([]cloud.Lock, error) {
	a.calls++
	if len(a.errs) > 0 {
		err := a.errs[0]
		a.errs = a.errs[1:]
		if err != nil {
			return nil, err
		}
	}
	return []cloud.Lock{{ID: "lock1"}}, nil
}

func (a *scriptedAPI) Command(context.Context, string, loqed.BoltState) error {
	a.commands++
	if a.token == "old" {
		return loqed.ErrUnauthorized
	}
	return nil
}

type fakeTokens struct{ token, next string }

func (f *fakeTokens) Token(context.Context) (string, error) { return f.token, nil }
func (f *fakeTokens) Invalidate(_ context.Context, rejected string) (string, error) {
	if f.next == "" {
		return "", loqed.ErrUnauthorized
	}
	f.token = f.next
	return f.next, nil
}

func newHub(now *time.Time, tokens TokenSource, apis map[string]*scriptedAPI) *CloudHub {
	clock := func() time.Time { return *now }
	return NewCloudHub(NewBudget(10, 12*time.Hour, clock), tokens, func(tok string) CloudAPI {
		a := apis[tok]
		a.token = tok
		return a
	}, clock, slog.New(slog.DiscardHandler))
}

func TestHubCoalescesRequests(t *testing.T) {
	now := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	api := &scriptedAPI{}
	h := newHub(&now, &fakeTokens{token: "tok"}, map[string]*scriptedAPI{"tok": api})
	ctx := context.Background()
	for range 3 {
		if _, err := h.Locks(ctx, PriorityRefresh); err != nil {
			t.Fatal(err)
		}
	}
	if api.calls != 1 {
		t.Fatalf("calls %d", api.calls)
	}
	now = now.Add(31 * time.Second)
	_, _ = h.Locks(ctx, PriorityRefresh)
	if api.calls != 2 || h.Token() != "tok" {
		t.Fatalf("calls %d token %q", api.calls, h.Token())
	}
}

func TestHubReauthenticatesOnce(t *testing.T) {
	now := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	old := &scriptedAPI{errs: []error{loqed.ErrUnauthorized}}
	fresh := &scriptedAPI{}
	h := newHub(&now, &fakeTokens{token: "old", next: "new"}, map[string]*scriptedAPI{"old": old, "new": fresh})
	locks, err := h.Locks(context.Background(), PriorityRefresh)
	if err != nil || len(locks) != 1 || fresh.calls != 1 || h.Token() != "new" {
		t.Fatalf("locks %v err %v fresh %d token %q", locks, err, fresh.calls, h.Token())
	}
}

func TestHubRateLimitBlocksBudget(t *testing.T) {
	now := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	api := &scriptedAPI{errs: []error{loqed.ErrRateLimited}}
	h := newHub(&now, &fakeTokens{token: "tok"}, map[string]*scriptedAPI{"tok": api})
	if _, err := h.Locks(context.Background(), PriorityRefresh); !errors.Is(err, loqed.ErrRateLimited) {
		t.Fatalf("got %v", err)
	}
	now = now.Add(time.Hour)
	if _, err := h.Locks(context.Background(), PriorityRefresh); !errors.Is(err, ErrCloudBlocked) {
		t.Fatalf("got %v", err)
	}
	if api.calls != 1 {
		t.Fatalf("blocked hub must not call the API: %d", api.calls)
	}
}

func TestHubCommandReauthenticates(t *testing.T) {
	now := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	old, fresh := &scriptedAPI{}, &scriptedAPI{}
	h := newHub(&now, &fakeTokens{token: "old", next: "new"}, map[string]*scriptedAPI{"old": old, "new": fresh})
	if err := h.Command(context.Background(), "lock1", loqed.BoltNightLock); err != nil {
		t.Fatal(err)
	}
	if old.commands != 1 || fresh.commands != 1 {
		t.Fatalf("old %d fresh %d", old.commands, fresh.commands)
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/gateway/`
Expected: FAIL — `undefined: NewBudget`.

- [ ] **Step 3: Implement `budget.go`**

```go
// Package gateway runs one supervisor per lock: local-first operation,
// cloud fallback, offline handling, and the shared cloud request budget.
package gateway

import (
	"errors"
	"sync"
	"time"
)

type Priority int

const (
	PriorityRefresh Priority = iota // credential refresh needed to reach local mode
	PriorityConfirm                 // confirm a command sent via cloud
	PriorityBackground              // periodic cloud-mode state poll
)

var (
	ErrBudgetExhausted = errors.New("gateway: cloud request budget exhausted")
	ErrDeferred        = errors.New("gateway: background cloud poll deferred to spread the budget")
	ErrCloudBlocked    = errors.New("gateway: cloud reads suspended after LOQED rate limiting")
)

const backgroundReserve = 2

// Budget limits GET /api/locks/ calls per rolling window, account-wide.
type Budget struct {
	limit  int
	window time.Duration
	now    func() time.Time

	mu             sync.Mutex
	calls          []time.Time
	lastBackground time.Time
	blockedUntil   time.Time
}

func NewBudget(limit int, window time.Duration, now func() time.Time) *Budget {
	return &Budget{limit: limit, window: window, now: now}
}

func (b *Budget) Take(p Priority) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	if now.Before(b.blockedUntil) {
		return ErrCloudBlocked
	}
	b.prune(now)
	remaining := b.limit - len(b.calls)
	if remaining <= 0 {
		return ErrBudgetExhausted
	}
	if p == PriorityBackground {
		if remaining <= min(backgroundReserve, b.limit-1) {
			return ErrBudgetExhausted
		}
		spacing := b.window / time.Duration(b.limit)
		if !b.lastBackground.IsZero() && now.Sub(b.lastBackground) < spacing {
			return ErrDeferred
		}
		b.lastBackground = now
	}
	b.calls = append(b.calls, now)
	return nil
}

func (b *Budget) Block(d time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.blockedUntil = b.now().Add(d)
}

func (b *Budget) Remaining() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.prune(b.now())
	return b.limit - len(b.calls)
}

func (b *Budget) prune(now time.Time) {
	cut := 0
	for cut < len(b.calls) && now.Sub(b.calls[cut]) >= b.window {
		cut++
	}
	b.calls = b.calls[cut:]
}
```

- [ ] **Step 4: Implement `cloudhub.go`**

```go
package gateway

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/cloud"
)

type CloudAPI interface {
	ListLocks(ctx context.Context) ([]cloud.Lock, error)
	Command(ctx context.Context, lockID string, s loqed.BoltState) error
}

type TokenSource interface {
	Token(ctx context.Context) (string, error)
	Invalidate(ctx context.Context, rejected string) (string, error)
}

const (
	RateLimitBackoff = 12 * time.Hour
	coalesceWindow   = 30 * time.Second
	cloudTimeout     = 15 * time.Second
)

// CloudHub serializes cloud access: one ListLocks result serves every lock,
// reads are budgeted, and rejected tokens are replaced once.
type CloudHub struct {
	budget *Budget
	tokens TokenSource
	newAPI func(token string) CloudAPI
	now    func() time.Time
	log    *slog.Logger

	mu     sync.Mutex
	api    CloudAPI
	token  string
	last   []cloud.Lock
	lastAt time.Time
}

func NewCloudHub(b *Budget, tokens TokenSource, newAPI func(token string) CloudAPI, now func() time.Time, log *slog.Logger) *CloudHub {
	return &CloudHub{budget: b, tokens: tokens, newAPI: newAPI, now: now, log: log}
}

// Token is the token currently in use ("" before the first call).
func (h *CloudHub) Token() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.token
}

// Locks returns the account's locks. Callers must not modify the slice.
func (h *CloudHub) Locks(ctx context.Context, p Priority) ([]cloud.Lock, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.last != nil && h.now().Sub(h.lastAt) < coalesceWindow {
		return h.last, nil
	}
	if err := h.budget.Take(p); err != nil {
		return nil, err
	}
	api, err := h.clientLocked(ctx)
	if err != nil {
		return nil, err
	}
	locks, err := listLocks(ctx, api)
	if errors.Is(err, loqed.ErrUnauthorized) {
		api, rerr := h.reauthLocked(ctx)
		if rerr != nil {
			return nil, errors.Join(err, rerr)
		}
		if err = h.budget.Take(p); err == nil {
			locks, err = listLocks(ctx, api)
		}
	}
	if errors.Is(err, loqed.ErrRateLimited) {
		h.budget.Block(RateLimitBackoff)
		h.log.Error("LOQED cloud rate limit reached; cloud reads suspended for 12h", "err", err)
	}
	if err != nil {
		return nil, err
	}
	h.last, h.lastAt = locks, h.now()
	return locks, nil
}

func (h *CloudHub) Command(ctx context.Context, lockID string, s loqed.BoltState) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	api, err := h.clientLocked(ctx)
	if err != nil {
		return err
	}
	err = command(ctx, api, lockID, s)
	if errors.Is(err, loqed.ErrUnauthorized) {
		api, rerr := h.reauthLocked(ctx)
		if rerr != nil {
			return errors.Join(err, rerr)
		}
		err = command(ctx, api, lockID, s)
	}
	return err
}

func (h *CloudHub) clientLocked(ctx context.Context) (CloudAPI, error) {
	if h.api != nil {
		return h.api, nil
	}
	tok, err := h.tokens.Token(ctx)
	if err != nil {
		return nil, err
	}
	h.token, h.api = tok, h.newAPI(tok)
	return h.api, nil
}

func (h *CloudHub) reauthLocked(ctx context.Context) (CloudAPI, error) {
	tok, err := h.tokens.Invalidate(ctx, h.token)
	if err != nil {
		return nil, err
	}
	h.token, h.api = tok, h.newAPI(tok)
	return h.api, nil
}

func listLocks(ctx context.Context, api CloudAPI) ([]cloud.Lock, error) {
	ctx, cancel := context.WithTimeout(ctx, cloudTimeout)
	defer cancel()
	return api.ListLocks(ctx)
}

func command(ctx context.Context, api CloudAPI, lockID string, s loqed.BoltState) error {
	ctx, cancel := context.WithTimeout(ctx, cloudTimeout)
	defer cancel()
	return api.Command(ctx, lockID, s)
}
```

- [ ] **Step 5: Run tests**

Run: `go test ./internal/gateway/ -v -race`
Expected: PASS (7 tests).

- [ ] **Step 6: Commit**

```bash
git add internal/gateway
git commit -m "gateway: add cloud request budget and coalescing cloud hub

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 8: Credential refresher and lock records

**Files:**
- Create: `internal/gateway/refresher.go`, `internal/gateway/records.go`, `internal/gateway/refresher_test.go`

**Interfaces:**
- Consumes: `CloudHub` (via `LockLister`), `store.Store`, `store.FromCloud`, `store.TokenHash`, `config.LockSetting`, `config.LockSettingsMap`.
- Produces:
  - `type gateway.Reason string`: `ReasonUnauthorized="unauthorized"`, `ReasonUnreachable="unreachable"`
  - `var gateway.ErrRefreshThrottled`
  - `type gateway.LockLister interface{ Locks(ctx, Priority) ([]cloud.Lock, error); Token() string }`
  - `func gateway.NewRefresher(c LockLister, st *store.Store, now func() time.Time) *Refresher`; `(*Refresher).RefreshAll(ctx) ([]store.LockRecord, error)`; `(*Refresher).Refresh(ctx, lockID string, reason Reason) (store.LockRecord, error)` (throttled to once per 5 min per lock+reason)
  - `func gateway.SettingFor(settings config.LockSettingsMap, rec store.LockRecord) config.LockSetting` (by id, then name)
  - `func gateway.ApplySetting(rec store.LockRecord, s config.LockSetting) store.LockRecord`
  - `func gateway.Select(records []store.LockRecord, allow []string) (selected []store.LockRecord, missing []string)`

- [ ] **Step 1: Write failing tests**

`internal/gateway/refresher_test.go`:

```go
package gateway

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/t3hk0d3/go-loqed/cloud"
	"github.com/t3hk0d3/go-loqed/internal/config"
	"github.com/t3hk0d3/go-loqed/internal/store"
)

type listerStub struct {
	locks []cloud.Lock
	calls int
}

func (l *listerStub) Locks(context.Context, Priority) ([]cloud.Lock, error) { l.calls++; return l.locks, nil }
func (l *listerStub) Token() string                                       { return "tok" }

func TestRefreshAllReplacesCache(t *testing.T) {
	st, _, _ := store.Open(filepath.Join(t.TempDir(), "locks.json"))
	id := 1
	l := &listerStub{locks: []cloud.Lock{{ID: "lock1", Name: "Front door", BridgeIP: "1.2.3.4", LocalID: &id, KeySecret: "k", BridgeKey: "b"}}}
	now := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	recs, err := NewRefresher(l, st, func() time.Time { return now }).RefreshAll(context.Background())
	if err != nil || len(recs) != 1 {
		t.Fatalf("%v %v", recs, err)
	}
	c := st.Snapshot()
	if len(c.Locks) != 1 || c.TokenSHA256 != store.TokenHash("tok") || !c.FetchedAt.Equal(now) {
		t.Fatalf("%+v", c)
	}
}

func TestRefreshIsThrottledPerLockAndReason(t *testing.T) {
	st, _, _ := store.Open(filepath.Join(t.TempDir(), "locks.json"))
	l := &listerStub{locks: []cloud.Lock{{ID: "lock1", BridgeIP: "1.2.3.4"}}}
	now := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	r := NewRefresher(l, st, func() time.Time { return now })
	ctx := context.Background()
	if rec, err := r.Refresh(ctx, "lock1", ReasonUnreachable); err != nil || rec.BridgeIP != "1.2.3.4" {
		t.Fatalf("%+v %v", rec, err)
	}
	if _, err := r.Refresh(ctx, "lock1", ReasonUnreachable); !errors.Is(err, ErrRefreshThrottled) {
		t.Fatalf("got %v", err)
	}
	if _, err := r.Refresh(ctx, "lock1", ReasonUnauthorized); err != nil {
		t.Fatalf("different reason must not be throttled: %v", err)
	}
	now = now.Add(5 * time.Minute)
	if _, err := r.Refresh(ctx, "lock1", ReasonUnreachable); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Refresh(ctx, "missing", ReasonUnreachable); err == nil {
		t.Fatal("expected error for lock no longer on the account")
	}
}

func TestSelectAndSettings(t *testing.T) {
	recs := []store.LockRecord{{ID: "a", Name: "Front door"}, {ID: "b", Name: "Back door"}}
	all, missing := Select(recs, nil)
	if len(all) != 2 || missing != nil {
		t.Fatalf("%v %v", all, missing)
	}
	sel, missing := Select(recs, []string{"Back door", "a", "Garage"})
	if len(sel) != 2 || sel[0].ID != "b" || sel[1].ID != "a" || len(missing) != 1 || missing[0] != "Garage" {
		t.Fatalf("%v %v", sel, missing)
	}

	id := 7
	settings := config.LockSettingsMap{"Front door": {BridgeIP: "10.0.0.9", LocalID: &id, KeyNames: config.KeyNames{1: "Alice"}}}
	s := SettingFor(settings, recs[0])
	got := ApplySetting(recs[0], s)
	if got.BridgeIP != "10.0.0.9" || *got.LocalID != 7 || got.Name != "Front door" {
		t.Fatalf("%+v", got)
	}
	if SettingFor(settings, recs[1]).BridgeIP != "" {
		t.Fatal("no setting expected for b")
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/gateway/ -run 'Refresh|Select'`
Expected: FAIL — `undefined: NewRefresher`.

- [ ] **Step 3: Implement `refresher.go`**

```go
package gateway

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/t3hk0d3/go-loqed/cloud"
	"github.com/t3hk0d3/go-loqed/internal/store"
)

type Reason string

const (
	ReasonUnauthorized Reason = "unauthorized"
	ReasonUnreachable  Reason = "unreachable"
)

var ErrRefreshThrottled = errors.New("gateway: credential refresh throttled")

const refreshInterval = 5 * time.Minute

type LockLister interface {
	Locks(ctx context.Context, p Priority) ([]cloud.Lock, error)
	Token() string
}

// Refresher reloads lock credentials from the cloud into the store.
type Refresher struct {
	cloud LockLister
	store *store.Store
	now   func() time.Time

	mu   sync.Mutex
	last map[string]time.Time
}

func NewRefresher(c LockLister, st *store.Store, now func() time.Time) *Refresher {
	return &Refresher{cloud: c, store: st, now: now, last: map[string]time.Time{}}
}

func (r *Refresher) RefreshAll(ctx context.Context) ([]store.LockRecord, error) {
	locks, err := r.cloud.Locks(ctx, PriorityRefresh)
	if err != nil {
		return nil, err
	}
	recs := make([]store.LockRecord, 0, len(locks))
	for _, l := range locks {
		recs = append(recs, store.FromCloud(l))
	}
	err = r.store.Update(func(c *store.Cache) {
		c.Locks = recs
		c.FetchedAt = r.now().UTC()
		c.TokenSHA256 = store.TokenHash(r.cloud.Token())
	})
	return recs, err
}

func (r *Refresher) Refresh(ctx context.Context, lockID string, reason Reason) (store.LockRecord, error) {
	key := lockID + "/" + string(reason)
	r.mu.Lock()
	if t, ok := r.last[key]; ok && r.now().Sub(t) < refreshInterval {
		r.mu.Unlock()
		return store.LockRecord{}, ErrRefreshThrottled
	}
	r.last[key] = r.now()
	r.mu.Unlock()

	recs, err := r.RefreshAll(ctx)
	if err != nil {
		return store.LockRecord{}, err
	}
	for _, rec := range recs {
		if rec.ID == lockID {
			return rec, nil
		}
	}
	return store.LockRecord{}, fmt.Errorf("gateway: lock %s is no longer on the account", lockID)
}
```

- [ ] **Step 4: Implement `records.go`**

```go
package gateway

import (
	"github.com/t3hk0d3/go-loqed/internal/config"
	"github.com/t3hk0d3/go-loqed/internal/store"
)

// SettingFor finds lock_settings for a lock by id, then by name.
func SettingFor(settings config.LockSettingsMap, rec store.LockRecord) config.LockSetting {
	if s, ok := settings[rec.ID]; ok {
		return s
	}
	if s, ok := settings[rec.Name]; ok {
		return s
	}
	return config.LockSetting{}
}

// ApplySetting overlays manual values onto cloud data.
func ApplySetting(rec store.LockRecord, s config.LockSetting) store.LockRecord {
	if s.BridgeIP != "" {
		rec.BridgeIP = s.BridgeIP
	}
	if s.BridgeKey != "" {
		rec.BridgeKey = s.BridgeKey
	}
	if s.KeySecret != "" {
		rec.KeySecret = s.KeySecret
	}
	if s.LocalID != nil {
		v := *s.LocalID
		rec.LocalID = &v
	}
	return rec
}

// Select applies the allow-list (ids or names, in allow-list order).
// An empty allow-list selects everything.
func Select(records []store.LockRecord, allow []string) (selected []store.LockRecord, missing []string) {
	if len(allow) == 0 {
		return append([]store.LockRecord(nil), records...), nil
	}
	seen := map[string]bool{}
	for _, a := range allow {
		found := false
		for _, r := range records {
			if r.ID == a || r.Name == a {
				found = true
				if !seen[r.ID] {
					seen[r.ID] = true
					selected = append(selected, r)
				}
			}
		}
		if !found {
			missing = append(missing, a)
		}
	}
	return selected, missing
}
```

- [ ] **Step 5: Run tests**

Run: `go test ./internal/gateway/ -v -race`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/gateway
git commit -m "gateway: add credential refresher and lock selection

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 9: Supervisor core and local mode

**Files:**
- Create: `internal/gateway/supervisor.go`, `internal/gateway/local.go`, `internal/gateway/cloudmode.go` (stub, replaced in Task 10), `internal/gateway/harness_test.go`, `internal/gateway/local_test.go`

**Interfaces:**
- Consumes: `bridge.*` types, `model.*`, `store.LockRecord`, `config.LockSetting`, `ApplySetting`, `Priority`, `Reason`.
- Produces:
  - `type gateway.BridgeAPI interface{ Status(ctx) (*bridge.Status, error); Command(ctx, bridge.Action) error; ListWebhooks(ctx) ([]bridge.Webhook, error); CreateWebhook(ctx, url string, t bridge.Triggers) error; DeleteWebhook(ctx, id int) error }` (satisfied by `*bridge.Client`)
  - `type gateway.CloudSource interface{ Locks(ctx, Priority) ([]cloud.Lock, error); Command(ctx, lockID string, s loqed.BoltState) error }` (satisfied by `*CloudHub`)
  - `type gateway.Publisher interface{ PublishState(id string, s model.State) error; PublishEvent(id string, e model.Event) error; PublishAvailability(id string, online bool) error }` (satisfied by `*hass.Client`)
  - `type gateway.Prober func(ctx context.Context, address string) error`; `func gateway.TCPProbe(ctx, address string) error`; `func gateway.BridgeAddress(bridgeIP string) string` (`ip` → `ip:80`; keeps an explicit `host:port`)
  - Messages: `gateway.BridgeEventMsg{Event bridge.Event}`, `gateway.CloudEventMsg{Event cloud.WebhookEvent}`, `gateway.CommandMsg{Command model.Command; At time.Time}`
  - `type gateway.Deps struct{ Publisher Publisher; Cloud CloudSource; Refresh func(ctx, lockID string, reason Reason) (store.LockRecord, error); NewBridge func(store.LockRecord) (BridgeAPI, error); Probe Prober; WebhookURL func(store.LockRecord) (string, error); CloudWebhooks bool; Now func() time.Time; Log *slog.Logger }`
  - `type gateway.Timing struct{ Liveness, Reconcile, OfflineRetry, UnknownRecheck, WebhookConfirm, CloudConfirm, CloudPoll, CommandMaxAge, EnrichWindow, RequestTimeout time.Duration; FailureThreshold int }`; `func gateway.DefaultTiming(liveness, reconcile time.Duration) Timing`
  - `func gateway.NewSupervisor(rec store.LockRecord, setting config.LockSetting, d Deps, t Timing) *Supervisor`; methods `ID() string`, `Record() store.LockRecord`, `BridgeKey() ([]byte, bool)`, `Deliver(msg any) bool`, `Health() Health`, `Run(ctx)`
  - `type gateway.Health struct{ Mode model.Mode; Available bool; LastEventAt *time.Time }` (JSON `mode`, `available`, `last_event_at`)
  - Unexported, used by later tasks: `start`, `tick`, `handle`, `publish`, `setMode`, `setRecord`, `ensureBridge`, `tryEnterLocal`, `localFailure`, `refreshAndRebuild`, `recordEvent`, `keyName`, `reqCtx`, `available`; fields `mode`, `state`, `failures`, `confirmAt`, `lastBridgeEvent`, `lastCloudEventAt`, `nextProbe`, `nextReconcile`, `nextCloudPoll`, `nextOfflineRetry`.

- [ ] **Step 1: Write the test harness**

`internal/gateway/harness_test.go`:

```go
package gateway

import (
	"context"
	"log/slog"
	"testing"
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/bridge"
	"github.com/t3hk0d3/go-loqed/cloud"
	"github.com/t3hk0d3/go-loqed/internal/config"
	"github.com/t3hk0d3/go-loqed/internal/model"
	"github.com/t3hk0d3/go-loqed/internal/store"
)

type fakeBridge struct {
	status      bridge.Status
	statusErr   error
	statusCalls int
	commandErrs []error // consumed per call; empty = success
	commands    []bridge.Action
	hooks       []bridge.Webhook
	listErr     error
	created     []string
	deleted     []int
}

func (f *fakeBridge) Status(context.Context) (*bridge.Status, error) {
	f.statusCalls++
	if f.statusErr != nil {
		return nil, f.statusErr
	}
	st := f.status
	return &st, nil
}

func (f *fakeBridge) Command(_ context.Context, a bridge.Action) error {
	f.commands = append(f.commands, a)
	if len(f.commandErrs) > 0 {
		err := f.commandErrs[0]
		f.commandErrs = f.commandErrs[1:]
		return err
	}
	return nil
}

func (f *fakeBridge) ListWebhooks(context.Context) ([]bridge.Webhook, error) { return f.hooks, f.listErr }

func (f *fakeBridge) CreateWebhook(_ context.Context, url string, _ bridge.Triggers) error {
	f.created = append(f.created, url)
	f.hooks = append(f.hooks, bridge.Webhook{ID: loqed.Int(100 + len(f.hooks)), URL: url})
	return nil
}

func (f *fakeBridge) DeleteWebhook(_ context.Context, id int) error {
	f.deleted = append(f.deleted, id)
	return nil
}

type fakeCloud struct {
	locks      []cloud.Lock
	err        error
	calls      []Priority
	commands   []loqed.BoltState
	commandErr error
}

func (f *fakeCloud) Locks(_ context.Context, p Priority) ([]cloud.Lock, error) {
	f.calls = append(f.calls, p)
	if f.err != nil {
		return nil, f.err
	}
	return f.locks, nil
}

func (f *fakeCloud) Command(_ context.Context, _ string, s loqed.BoltState) error {
	f.commands = append(f.commands, s)
	return f.commandErr
}

type fakePub struct {
	states []model.State
	events []model.Event
	avail  []bool
}

func (f *fakePub) PublishState(_ string, s model.State) error     { f.states = append(f.states, s); return nil }
func (f *fakePub) PublishEvent(_ string, e model.Event) error     { f.events = append(f.events, e); return nil }
func (f *fakePub) PublishAvailability(_ string, on bool) error    { f.avail = append(f.avail, on); return nil }

var t0 = time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)

func testRecord() store.LockRecord {
	id := 1
	return store.LockRecord{ID: "lock1", Name: "Front door", BridgeIP: "192.0.2.10", LocalID: &id,
		KeySecret: "SGFsbG8gd2VyZWxk", BridgeKey: "Ym9uam91ciBtb25kZQ=="}
}

type harness struct {
	t            *testing.T
	now          time.Time
	bridge       *fakeBridge
	cloud        *fakeCloud
	pub          *fakePub
	probeErr     error
	probes       []string
	refreshRec   *store.LockRecord
	refreshErr   error
	refreshes    []Reason
	bridgesBuilt int
	s            *Supervisor
}

func newHarness(t *testing.T, rec store.LockRecord, setting config.LockSetting, tweak ...func(*Deps)) *harness {
	t.Helper()
	h := &harness{
		t: t, now: t0,
		bridge: &fakeBridge{status: bridge.Status{BoltState: loqed.BoltDayLock, LockOnline: 1, BatteryPercentage: 80, BatteryVoltage: 10.4}},
		cloud:  &fakeCloud{locks: []cloud.Lock{{ID: "lock1", BoltState: loqed.BoltNightLock, BatteryPercentage: 70, Online: model.Ptr(true)}}},
		pub:    &fakePub{},
	}
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
		NewBridge: func(store.LockRecord) (BridgeAPI, error) { h.bridgesBuilt++; return h.bridge, nil },
		Probe: func(_ context.Context, addr string) error {
			h.probes = append(h.probes, addr)
			return h.probeErr
		},
		WebhookURL: func(r store.LockRecord) (string, error) { return "http://10.0.0.5:8099/webhook/" + r.ID, nil },
		Now:        func() time.Time { return h.now },
		Log:        slog.New(slog.DiscardHandler),
	}
	for _, f := range tweak {
		f(&d)
	}
	h.s = NewSupervisor(rec, setting, d, DefaultTiming(60*time.Second, 24*time.Hour))
	return h
}

func (h *harness) start()                  { h.s.start(context.Background()) }
func (h *harness) advance(d time.Duration) { h.now = h.now.Add(d); h.s.tick(context.Background()) }
func (h *harness) send(msg any)            { h.s.handle(context.Background(), msg) }

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
```

- [ ] **Step 2: Write failing local-mode tests**

`internal/gateway/local_test.go`:

```go
package gateway

import (
	"testing"
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/bridge"
	"github.com/t3hk0d3/go-loqed/internal/config"
	"github.com/t3hk0d3/go-loqed/internal/model"
)

func TestStartsLocalAndRegistersWebhook(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	if h.s.mode != model.ModeLocal || h.lock() != "UNLOCKED" || !h.available() || h.state().Mode != model.ModeLocal {
		t.Fatalf("mode %s lock %s", h.s.mode, h.lock())
	}
	if len(h.bridge.created) != 1 || h.bridge.created[0] != "http://10.0.0.5:8099/webhook/lock1" {
		t.Fatalf("created %v", h.bridge.created)
	}
	if h.state().BatteryPercentage == nil || *h.state().BatteryPercentage != 80 {
		t.Fatalf("battery %+v", h.state())
	}
}

func TestKeepsCurrentWebhookAndDeletesStaleOnes(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.bridge.hooks = []bridge.Webhook{
		{ID: 1, URL: "http://10.0.0.9:8099/webhook/lock1"},     // old gateway IP
		{ID: 2, URL: "http://10.0.0.5:8099/webhook/lock1"},     // current
		{ID: 3, URL: "http://ha.local:8123/api/webhook/abcdef"}, // someone else's
	}
	h.start()
	if len(h.bridge.created) != 0 || len(h.bridge.deleted) != 1 || h.bridge.deleted[0] != 1 {
		t.Fatalf("created %v deleted %v", h.bridge.created, h.bridge.deleted)
	}
}

func TestBridgeEventsUpdateStateAndEmitEvents(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.send(BridgeEventMsg{Event: bridge.GoToStateEvent{EventType: "GO_TO_STATE_TOUCH_TO_LOCK", GoToState: loqed.BoltNightLock, KeyLocalID: model.Ptr(255)}})
	if h.lock() != "LOCKING" || h.pub.events[0].EventType != model.EventLocking || h.pub.events[0].Source != "touch" {
		t.Fatalf("lock %s events %+v", h.lock(), h.pub.events)
	}
	h.send(BridgeEventMsg{Event: bridge.StateReachedEvent{EventType: "STATE_CHANGED_NIGHT_LOCK", RequestedState: loqed.BoltNightLock, KeyLocalID: model.Ptr(255)}})
	s := h.state()
	if h.lock() != "LOCKED" || s.BoltState != loqed.BoltNightLock || s.LastEvent != "STATE_CHANGED_NIGHT_LOCK" || s.LastKeyID != nil || s.LastEventAt == nil {
		t.Fatalf("state %+v", s)
	}
	if ev := h.pub.events[1]; ev.EventType != model.EventLocked || ev.KeyLocalID != nil || ev.KeyName != nil {
		t.Fatalf("event %+v", ev)
	}
}

func TestKeyNamesFromSettings(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{KeyNames: config.KeyNames{3: "Alice"}})
	h.start()
	h.send(BridgeEventMsg{Event: bridge.StateReachedEvent{EventType: "STATE_CHANGED_LATCH", RequestedState: loqed.BoltDayLock, KeyLocalID: model.Ptr(3)}})
	if n := h.state().LastKeyName; n == nil || *n != "Alice" {
		t.Fatalf("state %+v", h.state())
	}
	if n := h.pub.events[0].KeyName; n == nil || *n != "Alice" {
		t.Fatalf("event %+v", h.pub.events[0])
	}
}

func TestOnlineAndBatteryEventsTrackLockOnline(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.send(BridgeEventMsg{Event: bridge.OnlineEvent{WifiStrength: model.Ptr(70), BLEStrength: model.Ptr(-1)}})
	if h.state().LockOnline || h.available() {
		t.Fatal("ble -1 must mark the lock offline")
	}
	h.send(BridgeEventMsg{Event: bridge.BatteryEvent{BatteryPercentage: 55}})
	if !h.state().LockOnline || *h.state().BatteryPercentage != 55 || !h.available() {
		t.Fatalf("battery report must mark the lock online: %+v", h.state())
	}
	h.send(BridgeEventMsg{Event: bridge.BatteryEvent{BatteryPercentage: -1}})
	if h.state().LockOnline || *h.state().BatteryPercentage != 55 {
		t.Fatalf("battery -1 means offline and must not overwrite the level: %+v", h.state())
	}
	if len(h.pub.events) != 0 {
		t.Fatal("battery/online reports must not emit lock events")
	}
}

func TestStatusOnlyOnReconcileSchedule(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	if h.bridge.statusCalls != 1 {
		t.Fatalf("status calls %d", h.bridge.statusCalls)
	}
	for range 10 {
		h.advance(60 * time.Second)
	}
	if h.bridge.statusCalls != 1 || len(h.probes) != 10 || h.probes[0] != "192.0.2.10:80" {
		t.Fatalf("status %d probes %v", h.bridge.statusCalls, h.probes)
	}
	h.advance(24 * time.Hour)
	if h.bridge.statusCalls != 2 {
		t.Fatalf("expected daily reconcile, status calls %d", h.bridge.statusCalls)
	}
}

func TestWebhookCountsAsLiveness(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.advance(50 * time.Second)
	h.send(BridgeEventMsg{Event: bridge.OnlineEvent{BLEStrength: model.Ptr(40)}})
	h.advance(20 * time.Second) // 70s after start, 20s after the webhook
	if len(h.probes) != 0 {
		t.Fatalf("probe should be postponed by the webhook: %v", h.probes)
	}
}

func TestUnknownBoltRecheckedEveryTenMinutes(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.bridge.status.BoltState = loqed.BoltUnknown
	h.start()
	if h.state().Lock != nil {
		t.Fatal("unknown bolt must publish lock=null")
	}
	h.advance(5 * time.Minute)
	if h.bridge.statusCalls != 1 {
		t.Fatalf("too early: %d", h.bridge.statusCalls)
	}
	h.advance(5 * time.Minute)
	if h.bridge.statusCalls != 2 {
		t.Fatalf("expected recheck: %d", h.bridge.statusCalls)
	}
}

func TestBridgeAuthErrorRefreshesCredentials(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.bridge.listErr = loqed.ErrUnauthorized
	h.start()
	if len(h.refreshes) != 1 || h.refreshes[0] != ReasonUnauthorized || h.bridgesBuilt != 2 {
		t.Fatalf("refreshes %v bridges %d", h.refreshes, h.bridgesBuilt)
	}
	if h.s.mode != model.ModeLocal {
		t.Fatal("status works without keys; stay local")
	}
}

func TestBridgeAddress(t *testing.T) {
	if BridgeAddress("192.0.2.10") != "192.0.2.10:80" || BridgeAddress("127.0.0.1:8080") != "127.0.0.1:8080" {
		t.Fatal("BridgeAddress")
	}
}
```

- [ ] **Step 3: Run to verify failure**

Run: `go test ./internal/gateway/ -run 'Local|Webhook|Bridge|Key|Online|Status|Unknown'`
Expected: FAIL — `undefined: NewSupervisor`.

- [ ] **Step 4: Implement `supervisor.go`**

```go
package gateway

import (
	"context"
	"encoding/base64"
	"errors"
	"log/slog"
	"net"
	"strings"
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

type CloudSource interface {
	Locks(ctx context.Context, p Priority) ([]cloud.Lock, error)
	Command(ctx context.Context, lockID string, s loqed.BoltState) error
}

type Publisher interface {
	PublishState(lockID string, s model.State) error
	PublishEvent(lockID string, e model.Event) error
	PublishAvailability(lockID string, online bool) error
}

type Prober func(ctx context.Context, address string) error

// TCPProbe checks the bridge is reachable without an HTTP request.
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
type CloudEventMsg struct{ Event cloud.WebhookEvent }
type CommandMsg struct {
	Command model.Command
	At      time.Time
}

type Deps struct {
	Publisher     Publisher
	Cloud         CloudSource
	Refresh       func(ctx context.Context, lockID string, reason Reason) (store.LockRecord, error)
	NewBridge     func(rec store.LockRecord) (BridgeAPI, error)
	Probe         Prober
	WebhookURL    func(rec store.LockRecord) (string, error)
	CloudWebhooks bool
	Now           func() time.Time
	Log           *slog.Logger
}

type Timing struct {
	Liveness         time.Duration
	Reconcile        time.Duration
	OfflineRetry     time.Duration
	UnknownRecheck   time.Duration
	WebhookConfirm   time.Duration
	CloudConfirm     time.Duration
	CloudPoll        time.Duration
	CommandMaxAge    time.Duration
	EnrichWindow     time.Duration
	RequestTimeout   time.Duration
	FailureThreshold int
}

func DefaultTiming(liveness, reconcile time.Duration) Timing {
	return Timing{
		Liveness: liveness, Reconcile: reconcile,
		OfflineRetry: 5 * time.Minute, UnknownRecheck: 10 * time.Minute,
		WebhookConfirm: 10 * time.Second, CloudConfirm: 5 * time.Second, CloudPoll: time.Minute,
		CommandMaxAge: 10 * time.Second, EnrichWindow: 30 * time.Second, RequestTimeout: 5 * time.Second,
		FailureThreshold: 3,
	}
}

type Health struct {
	Mode        model.Mode `json:"mode"`
	Available   bool       `json:"available"`
	LastEventAt *time.Time `json:"last_event_at"`
}

type recentEvent struct {
	eventType string
	at        time.Time
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

	bridge           BridgeAPI
	mode             model.Mode
	state            model.State
	failures         int
	nextProbe        time.Time
	nextReconcile    time.Time
	nextCloudPoll    time.Time
	nextOfflineRetry time.Time
	lastUnknownCheck time.Time
	confirmAt        time.Time
	lastBridgeEvent  *recentEvent
	lastCloudEventAt time.Time
}

func NewSupervisor(rec store.LockRecord, setting config.LockSetting, d Deps, t Timing) *Supervisor {
	return &Supervisor{
		id: rec.ID, d: d, t: t, setting: setting, in: make(chan any, 64),
		log:   d.Log.With("lock", rec.Name, "lock_id", rec.ID),
		rec:   ApplySetting(rec, setting),
		mode:  model.ModeOffline,
		state: model.State{BoltState: loqed.BoltUnknown, Mode: model.ModeOffline},
	}
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
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			s.tick(ctx)
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
	now := s.d.Now()
	switch s.mode {
	case model.ModeLocal:
		s.tickLocal(ctx, now)
	case model.ModeCloud:
		s.tickCloud(ctx, now)
	case model.ModeOffline:
		s.tickOffline(ctx, now)
	}
}

func (s *Supervisor) handle(ctx context.Context, m any) {
	switch m := m.(type) {
	case BridgeEventMsg:
		s.onBridgeEvent(ctx, m.Event)
	case CloudEventMsg:
		s.onCloudEvent(ctx, m.Event)
	}
}

func (s *Supervisor) reqCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, s.t.RequestTimeout)
}

func (s *Supervisor) available() bool { return s.mode != model.ModeOffline && s.state.LockOnline }

// publish sends the state document and availability, and updates Health.
func (s *Supervisor) publish() {
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
	s.mode, s.state.Mode, s.failures = m, m, 0
}

func (s *Supervisor) setRecord(rec store.LockRecord) {
	s.mu.Lock()
	s.rec = ApplySetting(rec, s.setting)
	s.mu.Unlock()
	s.bridge = nil
}

// recordEvent applies a lock event, publishes state and the HA event.
func (s *Supervisor) recordEvent(now time.Time, eventType string, rawKey *int, cloudKeyName string, t model.Transition, fromBridge bool) {
	s.state.Apply(t)
	s.state.StateStale = false
	key := model.NormalizeKeyID(rawKey)
	name := s.keyName(key, cloudKeyName)
	at := now.UTC().Truncate(time.Second)
	s.state.LastEvent, s.state.LastKeyID, s.state.LastKeyName, s.state.LastEventAt = eventType, key, name, &at
	if t.SetBolt || t.Event == model.EventJammed {
		s.confirmAt = time.Time{} // the command outcome arrived
	}
	if fromBridge {
		s.lastBridgeEvent = &recentEvent{eventType: strings.ToUpper(eventType), at: now}
	}
	s.publish()
	ev := model.Event{EventType: t.Event, Reason: eventType, Source: model.Source(eventType), KeyLocalID: key, KeyName: name}
	if err := s.d.Publisher.PublishEvent(s.id, ev); err != nil {
		s.log.Warn("publishing event failed", "err", err)
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
```


- [ ] **Step 5: Implement `local.go`**

```go
package gateway

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/bridge"
	"github.com/t3hk0d3/go-loqed/internal/model"
)

func (s *Supervisor) canLocal() bool { return s.Record().HasLocalCredentials() }

func (s *Supervisor) ensureBridge() bool {
	if s.bridge != nil {
		return true
	}
	b, err := s.d.NewBridge(s.Record())
	if err != nil {
		s.log.Error("cannot create bridge client", "err", err)
		return false
	}
	s.bridge = b
	return true
}

// tryEnterLocal fetches status and, on success, switches to local mode and
// makes sure our webhook is registered.
func (s *Supervisor) tryEnterLocal(ctx context.Context) bool {
	if !s.canLocal() || !s.ensureBridge() {
		return false
	}
	st, err := s.status(ctx)
	if err != nil {
		s.log.Debug("bridge not reachable", "err", err)
		return false
	}
	now := s.d.Now()
	s.setMode(model.ModeLocal)
	s.applyStatus(st)
	s.nextProbe = now.Add(s.t.Liveness)
	s.nextReconcile = now.Add(s.t.Reconcile)
	if err := s.ensureWebhook(ctx); err != nil {
		if errors.Is(err, loqed.ErrUnauthorized) && s.refreshAndRebuild(ctx, ReasonUnauthorized) {
			err = s.ensureWebhook(ctx)
		}
		if err != nil {
			s.log.Warn("could not register the webhook on the bridge; updates will wait for the next reconcile", "err", err)
		}
	}
	s.publish()
	return true
}

func (s *Supervisor) status(ctx context.Context) (*bridge.Status, error) {
	c, cancel := s.reqCtx(ctx)
	defer cancel()
	return s.bridge.Status(c)
}

func (s *Supervisor) applyStatus(st *bridge.Status) {
	s.state.BoltState = st.BoltState
	s.state.Lock = model.LockStateFor(st.BoltState)
	s.state.BatteryPercentage = model.Ptr(int(st.BatteryPercentage))
	s.state.BatteryVoltage = model.Ptr(float64(st.BatteryVoltage))
	s.state.WifiStrength = model.Ptr(int(st.WifiStrength))
	s.state.BLEStrength = model.Ptr(int(st.BLEStrength))
	s.state.LockOnline = st.LockOnline == 1
	s.state.StateStale = false
	if st.BoltState == loqed.BoltUnknown {
		s.lastUnknownCheck = s.d.Now()
	}
}

// ensureWebhook registers <private>/webhook/<id> and removes our stale
// registrations (same path, different host or port). Other webhooks stay.
func (s *Supervisor) ensureWebhook(ctx context.Context) error {
	want, err := s.d.WebhookURL(s.Record())
	if err != nil {
		return err
	}
	c, cancel := s.reqCtx(ctx)
	defer cancel()
	hooks, err := s.bridge.ListWebhooks(c)
	if err != nil {
		return err
	}
	suffix := "/webhook/" + s.id
	found := false
	for _, h := range hooks {
		if h.URL == want {
			found = true
			continue
		}
		if u, perr := url.Parse(h.URL); perr == nil && strings.HasSuffix(u.Path, suffix) {
			if err := s.bridge.DeleteWebhook(c, int(h.ID)); err != nil {
				s.log.Warn("could not delete a stale webhook", "webhook_id", int(h.ID), "err", err)
			} else {
				s.log.Info("deleted a stale webhook", "webhook_id", int(h.ID))
			}
		}
	}
	if found {
		return nil
	}
	return s.bridge.CreateWebhook(c, want, bridge.AllTriggers)
}

func (s *Supervisor) tickLocal(ctx context.Context, now time.Time) {
	if !s.confirmAt.IsZero() && !now.Before(s.confirmAt) {
		s.confirmAt = time.Time{}
		s.reconcile(ctx) // the command's webhook never arrived
		if s.mode != model.ModeLocal {
			return
		}
	}
	if !now.Before(s.nextProbe) {
		s.nextProbe = now.Add(s.t.Liveness)
		s.probeLocal(ctx)
		if s.mode != model.ModeLocal {
			return
		}
	}
	if s.state.BoltState == loqed.BoltUnknown && now.Sub(s.lastUnknownCheck) >= s.t.UnknownRecheck {
		s.reconcile(ctx)
		if s.mode != model.ModeLocal {
			return
		}
	}
	if !now.Before(s.nextReconcile) {
		s.reconcile(ctx)
	}
}

func (s *Supervisor) reconcile(ctx context.Context) {
	now := s.d.Now()
	s.nextReconcile = now.Add(s.t.Reconcile)
	if s.state.BoltState == loqed.BoltUnknown {
		s.lastUnknownCheck = now
	}
	st, err := s.status(ctx)
	if err != nil {
		s.localFailure(ctx, err)
		return
	}
	s.failures = 0
	s.applyStatus(st)
	s.publish()
}

func (s *Supervisor) probeLocal(ctx context.Context) {
	c, cancel := s.reqCtx(ctx)
	defer cancel()
	if err := s.d.Probe(c, BridgeAddress(s.Record().BridgeIP)); err != nil {
		s.localFailure(ctx, err)
		return
	}
	s.failures = 0
}

// localFailure counts a failed bridge interaction. After FailureThreshold
// failures it tries a cloud credential refresh (the IP may have changed,
// unless pinned in lock_settings) and otherwise falls back to cloud mode.
func (s *Supervisor) localFailure(ctx context.Context, err error) {
	if errors.Is(err, loqed.ErrUnauthorized) {
		s.refreshAndRebuild(ctx, ReasonUnauthorized)
		return
	}
	s.failures++
	s.log.Debug("bridge request failed", "failures", s.failures, "err", err)
	if s.failures < s.t.FailureThreshold {
		return
	}
	s.log.Warn("bridge unreachable", "err", err)
	if s.setting.BridgeIP == "" {
		oldIP := s.Record().BridgeIP
		rec, rerr := s.d.Refresh(ctx, s.id, ReasonUnreachable)
		switch {
		case rerr != nil:
			s.log.Debug("credential refresh not possible", "err", rerr)
		case rec.BridgeIP != oldIP:
			s.log.Info("bridge IP changed", "old", oldIP, "new", rec.BridgeIP)
			s.setRecord(rec)
			if s.tryEnterLocal(ctx) {
				return
			}
		}
	}
	s.enterCloud(ctx)
}

func (s *Supervisor) refreshAndRebuild(ctx context.Context, reason Reason) bool {
	rec, err := s.d.Refresh(ctx, s.id, reason)
	if err != nil {
		s.log.Warn("credential refresh failed", "reason", reason, "err", err)
		return false
	}
	s.setRecord(rec)
	return s.ensureBridge()
}

func (s *Supervisor) onBridgeEvent(ctx context.Context, ev bridge.Event) {
	if s.mode != model.ModeLocal && !s.tryEnterLocal(ctx) {
		return
	}
	now := s.d.Now()
	s.failures = 0
	s.nextProbe = now.Add(s.t.Liveness) // a webhook proves the bridge is alive
	switch e := ev.(type) {
	case bridge.StateReachedEvent:
		s.state.LockOnline = true
		s.recordEvent(now, e.EventType, e.KeyLocalID, "", model.FromStateReached(e.EventType, e.RequestedState), true)
	case bridge.GoToStateEvent:
		s.recordEvent(now, e.EventType, e.KeyLocalID, "", model.FromGoTo(e.GoToState, s.state.Lock), true)
	case bridge.BatteryEvent:
		if e.BatteryPercentage >= 0 {
			s.state.BatteryPercentage = model.Ptr(e.BatteryPercentage)
		}
		if e.WifiStrength != nil {
			s.state.WifiStrength = e.WifiStrength
		}
		if e.BLEStrength != nil {
			s.state.BLEStrength = e.BLEStrength
			s.state.LockOnline = *e.BLEStrength != -1
		} else {
			s.state.LockOnline = e.BatteryPercentage != -1
		}
		s.publish()
	case bridge.OnlineEvent:
		if e.WifiStrength != nil {
			s.state.WifiStrength = e.WifiStrength
		}
		if e.BLEStrength != nil {
			s.state.BLEStrength = e.BLEStrength
			s.state.LockOnline = *e.BLEStrength != -1
		}
		s.publish()
	}
}
```

- [ ] **Step 6: Add the cloud-mode stub**

`internal/gateway/cloudmode.go` (replaced entirely in Task 10):

```go
package gateway

import (
	"context"
	"time"

	"github.com/t3hk0d3/go-loqed/cloud"
	"github.com/t3hk0d3/go-loqed/internal/model"
)

func (s *Supervisor) enterCloud(ctx context.Context) {
	s.setMode(model.ModeCloud)
	s.publish()
}

func (s *Supervisor) tickCloud(ctx context.Context, now time.Time)        {}
func (s *Supervisor) tickOffline(ctx context.Context, now time.Time)      {}
func (s *Supervisor) onCloudEvent(ctx context.Context, e cloud.WebhookEvent) {}
```

- [ ] **Step 7: Run tests**

Run: `gofmt -w internal/gateway && go vet ./internal/gateway/ && go test ./internal/gateway/ -v -race`
Expected: PASS. (`toCloud` in the harness is not used yet; vet does not flag unused methods.)

- [ ] **Step 8: Commit**

```bash
git add internal/gateway
git commit -m "gateway: add lock supervisor with local mode

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 10: Cloud fallback and offline mode

**Files:**
- Replace: `internal/gateway/cloudmode.go`
- Create: `internal/gateway/failover_test.go`

**Interfaces:**
- Consumes: everything from Task 9; `ErrDeferred`, `ErrBudgetExhausted`, `ErrCloudBlocked`.
- Produces: `enterCloud(ctx)`, `enterOffline()`, `tickCloud`, `tickOffline`, `pollCloud(ctx, Priority) bool`, `applyCloudLock(cloud.Lock)`, `probeOK(ctx) bool`, `cloudPollInterval(now) time.Duration`. `onCloudEvent` stays a stub until Task 12.

- [ ] **Step 1: Write failing tests**

`internal/gateway/failover_test.go`:

```go
package gateway

import (
	"testing"
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/internal/config"
	"github.com/t3hk0d3/go-loqed/internal/model"
	"github.com/t3hk0d3/go-loqed/internal/store"
)

func TestThreeFailuresSwitchToCloud(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.probeErr = loqed.ErrUnreachable
	h.advance(60 * time.Second)
	h.advance(60 * time.Second)
	if h.s.mode != model.ModeLocal {
		t.Fatal("two failures must not switch modes")
	}
	h.advance(60 * time.Second)
	if h.s.mode != model.ModeCloud || len(h.refreshes) != 1 || h.refreshes[0] != ReasonUnreachable {
		t.Fatalf("mode %s refreshes %v", h.s.mode, h.refreshes)
	}
	if len(h.cloud.calls) != 1 || h.cloud.calls[0] != PriorityBackground {
		t.Fatalf("cloud calls %v", h.cloud.calls)
	}
	if h.lock() != "LOCKED" || h.state().Mode != model.ModeCloud || !h.available() {
		t.Fatalf("lock %s state %+v", h.lock(), h.state())
	}
}

func TestIPChangeReconnectsLocally(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	moved := testRecord()
	moved.BridgeIP = "192.0.2.77"
	h.refreshRec = &moved
	h.toCloudOrLocalAfterFailures()
	if h.s.mode != model.ModeLocal || h.s.Record().BridgeIP != "192.0.2.77" || h.bridgesBuilt != 2 {
		t.Fatalf("mode %s ip %s bridges %d", h.s.mode, h.s.Record().BridgeIP, h.bridgesBuilt)
	}
}

func TestPinnedBridgeIPSkipsRefresh(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{BridgeIP: "192.0.2.99"})
	h.start()
	h.toCloud()
	if len(h.refreshes) != 0 || h.probes[0] != "192.0.2.99:80" {
		t.Fatalf("refreshes %v probes %v", h.refreshes, h.probes)
	}
}

func TestCloudModeReturnsToLocal(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.toCloud()
	h.probeErr = nil
	h.advance(60 * time.Second)
	if h.s.mode != model.ModeLocal || h.lock() != "UNLOCKED" {
		t.Fatalf("mode %s lock %s", h.s.mode, h.lock())
	}
}

func TestCloudFailuresGoOfflineThenRetryEveryFiveMinutes(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.toCloud()
	h.cloud.err = loqed.ErrUnreachable
	for range 3 {
		h.advance(time.Minute)
	}
	if h.s.mode != model.ModeOffline || h.available() || h.state().Mode != model.ModeOffline {
		t.Fatalf("mode %s", h.s.mode)
	}
	h.cloud.err = nil
	calls := len(h.cloud.calls)
	h.advance(time.Minute)
	if h.s.mode != model.ModeOffline || len(h.cloud.calls) != calls {
		t.Fatal("must wait 5 minutes before retrying")
	}
	h.advance(4 * time.Minute)
	if h.s.mode != model.ModeCloud {
		t.Fatalf("mode %s", h.s.mode)
	}
}

func TestOfflineRetriesForever(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.toCloud()
	h.cloud.err = loqed.ErrUnreachable
	for range 3 {
		h.advance(time.Minute)
	}
	before := len(h.probes)
	for range 24 { // two hours
		h.advance(5 * time.Minute)
	}
	if h.s.mode != model.ModeOffline || len(h.probes)-before != 24 {
		t.Fatalf("mode %s probes %d", h.s.mode, len(h.probes)-before)
	}
}

func TestBudgetExhaustionMarksStateStale(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.toCloud()
	h.cloud.err = ErrBudgetExhausted
	h.advance(time.Minute)
	if !h.state().StateStale || h.s.mode != model.ModeCloud {
		t.Fatalf("stale %v mode %s", h.state().StateStale, h.s.mode)
	}
	h.cloud.err = ErrDeferred
	h.cloud.locks[0].BoltState = loqed.BoltDayLock
	h.advance(time.Minute)
	if h.s.mode != model.ModeCloud || h.lock() != "LOCKED" {
		t.Fatal("deferred polls change nothing")
	}
	h.cloud.err = nil
	h.advance(time.Minute)
	if h.state().StateStale || h.lock() != "UNLOCKED" {
		t.Fatalf("fresh poll must clear stale: %+v", h.state())
	}
}

func TestCloudOnlyLockStartsInCloud(t *testing.T) {
	h := newHarness(t, store.LockRecord{ID: "lock1", Name: "Pure"}, config.LockSetting{})
	h.start()
	if h.s.mode != model.ModeCloud || h.bridgesBuilt != 0 {
		t.Fatalf("mode %s bridges %d", h.s.mode, h.bridgesBuilt)
	}
	h.advance(60 * time.Second)
	if len(h.probes) != 0 {
		t.Fatal("cloud-only locks are never probed")
	}
}
```

Add to `harness_test.go`:

```go
// toCloudOrLocalAfterFailures fails 3 probes and lets the supervisor decide.
func (h *harness) toCloudOrLocalAfterFailures() {
	h.probeErr = loqed.ErrUnreachable
	for range 3 {
		h.advance(60 * time.Second)
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/gateway/ -run 'Failures|IPChange|Pinned|CloudMode|Offline|Budget|CloudOnly'`
Expected: FAIL — e.g. `TestThreeFailuresSwitchToCloud`: cloud calls `[]` (stub does not poll).

- [ ] **Step 3: Replace `cloudmode.go`**

```go
package gateway

import (
	"context"
	"errors"
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/cloud"
	"github.com/t3hk0d3/go-loqed/internal/model"
)

func (s *Supervisor) enterCloud(ctx context.Context) {
	now := s.d.Now()
	s.setMode(model.ModeCloud)
	s.nextProbe = now.Add(s.t.Liveness)
	s.nextCloudPoll = now.Add(s.t.CloudPoll)
	s.publish()
	s.pollCloud(ctx, PriorityBackground)
}

func (s *Supervisor) enterOffline() {
	s.setMode(model.ModeOffline)
	s.nextOfflineRetry = s.d.Now().Add(s.t.OfflineRetry)
	s.publish()
}

func (s *Supervisor) tickCloud(ctx context.Context, now time.Time) {
	if !now.Before(s.nextProbe) {
		s.nextProbe = now.Add(s.t.Liveness)
		if s.probeOK(ctx) && s.tryEnterLocal(ctx) {
			return
		}
	}
	if !s.confirmAt.IsZero() && !now.Before(s.confirmAt) {
		s.confirmAt = time.Time{}
		s.pollCloud(ctx, PriorityConfirm)
		if s.mode != model.ModeCloud {
			return
		}
	}
	if !now.Before(s.nextCloudPoll) {
		s.nextCloudPoll = now.Add(s.cloudPollInterval(now))
		s.pollCloud(ctx, PriorityBackground)
	}
}

// cloudPollInterval: with working cloud webhooks only a reconcile poll is
// needed; otherwise ask every minute and let the budget space the calls.
func (s *Supervisor) cloudPollInterval(now time.Time) time.Duration {
	if s.d.CloudWebhooks && !s.lastCloudEventAt.IsZero() && now.Sub(s.lastCloudEventAt) < s.t.Reconcile {
		return s.t.Reconcile
	}
	return s.t.CloudPoll
}

func (s *Supervisor) tickOffline(ctx context.Context, now time.Time) {
	if now.Before(s.nextOfflineRetry) {
		return
	}
	s.nextOfflineRetry = now.Add(s.t.OfflineRetry)
	if s.probeOK(ctx) && s.tryEnterLocal(ctx) {
		return
	}
	s.pollCloud(ctx, PriorityBackground)
}

func (s *Supervisor) probeOK(ctx context.Context) bool {
	if !s.canLocal() {
		return false
	}
	c, cancel := s.reqCtx(ctx)
	defer cancel()
	return s.d.Probe(c, BridgeAddress(s.Record().BridgeIP)) == nil
}

// pollCloud reads the lock from the cloud. It returns true on fresh data.
func (s *Supervisor) pollCloud(ctx context.Context, p Priority) bool {
	locks, err := s.d.Cloud.Locks(ctx, p)
	switch {
	case errors.Is(err, ErrDeferred):
		return false
	case errors.Is(err, ErrBudgetExhausted), errors.Is(err, ErrCloudBlocked), errors.Is(err, loqed.ErrRateLimited):
		if !s.state.StateStale {
			s.state.StateStale = true
			s.publish()
		}
		return false
	case err != nil:
		s.failures++
		s.log.Warn("cloud request failed", "failures", s.failures, "err", err)
		if s.mode == model.ModeCloud && s.failures >= s.t.FailureThreshold {
			s.enterOffline()
		}
		return false
	}
	for _, l := range locks {
		if l.ID != s.id {
			continue
		}
		if s.mode == model.ModeOffline {
			now := s.d.Now()
			s.setMode(model.ModeCloud)
			s.nextProbe, s.nextCloudPoll = now.Add(s.t.Liveness), now.Add(s.t.CloudPoll)
		}
		s.failures = 0
		s.applyCloudLock(l)
		s.publish()
		return true
	}
	s.log.Warn("lock is missing from the cloud lock list")
	return false
}

func (s *Supervisor) applyCloudLock(l cloud.Lock) {
	s.state.BoltState = l.BoltState
	s.state.Lock = model.LockStateFor(l.BoltState)
	s.state.BatteryPercentage = model.Ptr(l.BatteryPercentage)
	s.state.LockOnline = l.Online == nil || *l.Online
	s.state.StateStale = false
}

// onCloudEvent is implemented in Task 12.
func (s *Supervisor) onCloudEvent(ctx context.Context, e cloud.WebhookEvent) {}
```

- [ ] **Step 4: Run tests**

Run: `go test ./internal/gateway/ -v -race`
Expected: PASS (all Task 7–10 tests).

- [ ] **Step 5: Commit**

```bash
git add internal/gateway
git commit -m "gateway: add cloud fallback and offline retry

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 11: Commands

**Files:**
- Create: `internal/gateway/commands.go`, `internal/gateway/commands_test.go`
- Modify: `internal/gateway/supervisor.go` (`handle`)

**Interfaces:**
- Consumes: `CommandMsg`, `model.Command.Target/Moving`, `localFailure`, `refreshAndRebuild`, `ensureBridge`.
- Produces: `onCommand(ctx, CommandMsg)`, `bridgeCommand(ctx, model.Command) error`, `commandViaCloud(ctx, model.Command)`.

- [ ] **Step 1: Write failing tests**

`internal/gateway/commands_test.go`:

```go
package gateway

import (
	"testing"
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/bridge"
	"github.com/t3hk0d3/go-loqed/internal/config"
	"github.com/t3hk0d3/go-loqed/internal/model"
)

func cmd(h *harness, c model.Command) CommandMsg { return CommandMsg{Command: c, At: h.now} }

func TestLocalCommandUsesBridge(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.send(cmd(h, model.CommandLock))
	if len(h.bridge.commands) != 1 || h.bridge.commands[0] != bridge.ActionLock || len(h.cloud.commands) != 0 {
		t.Fatalf("bridge %v cloud %v", h.bridge.commands, h.cloud.commands)
	}
	h.send(cmd(h, model.CommandOpen))
	h.send(cmd(h, model.CommandUnlock))
	if h.bridge.commands[1] != bridge.ActionOpen || h.bridge.commands[2] != bridge.ActionUnlock {
		t.Fatalf("bridge %v", h.bridge.commands)
	}
}

func TestStaleCommandIsDropped(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.send(CommandMsg{Command: model.CommandOpen, At: h.now.Add(-11 * time.Second)})
	if len(h.bridge.commands) != 0 || len(h.cloud.commands) != 0 {
		t.Fatal("a stale OPEN must never fire")
	}
}

func TestCommandFallsBackToCloudWhenBridgeUnreachable(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.bridge.commandErrs = []error{loqed.ErrUnreachable}
	h.send(cmd(h, model.CommandLock))
	if len(h.cloud.commands) != 1 || h.cloud.commands[0] != loqed.BoltNightLock || h.lock() != "LOCKING" {
		t.Fatalf("cloud %v lock %s", h.cloud.commands, h.lock())
	}
	if h.s.failures != 1 {
		t.Fatalf("failure must count toward health: %d", h.s.failures)
	}
}

func TestCommandAuthErrorRefreshesAndRetries(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.bridge.commandErrs = []error{loqed.ErrUnauthorized}
	h.send(cmd(h, model.CommandUnlock))
	if len(h.bridge.commands) != 2 || len(h.cloud.commands) != 0 || len(h.refreshes) != 1 || h.refreshes[0] != ReasonUnauthorized {
		t.Fatalf("bridge %v cloud %v refreshes %v", h.bridge.commands, h.cloud.commands, h.refreshes)
	}
}

func TestMissedWebhookTriggersStatusAfterTenSeconds(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.send(cmd(h, model.CommandLock))
	h.advance(9 * time.Second)
	if h.bridge.statusCalls != 1 {
		t.Fatal("too early")
	}
	h.advance(time.Second)
	if h.bridge.statusCalls != 2 {
		t.Fatalf("status calls %d", h.bridge.statusCalls)
	}
}

func TestWebhookConfirmationCancelsStatus(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.send(cmd(h, model.CommandLock))
	h.send(BridgeEventMsg{Event: bridge.StateReachedEvent{EventType: "STATE_CHANGED_NIGHT_LOCK", RequestedState: loqed.BoltNightLock}})
	h.advance(10 * time.Second)
	if h.bridge.statusCalls != 1 {
		t.Fatalf("status calls %d", h.bridge.statusCalls)
	}
}

func TestOfflineRejectsCommands(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.toCloud()
	h.cloud.err = loqed.ErrUnreachable
	for range 3 {
		h.advance(time.Minute)
	}
	h.send(cmd(h, model.CommandOpen))
	if len(h.cloud.commands) != 0 || len(h.bridge.commands) != 0 {
		t.Fatal("offline must reject commands")
	}
}

func TestCloudModeCommandConfirmPoll(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.toCloud()
	calls := len(h.cloud.calls)
	h.send(cmd(h, model.CommandUnlock))
	if len(h.cloud.commands) != 1 || h.cloud.commands[0] != loqed.BoltDayLock || h.lock() != "UNLOCKING" {
		t.Fatalf("cloud %v lock %s", h.cloud.commands, h.lock())
	}
	h.advance(5 * time.Second)
	if len(h.cloud.calls) != calls+1 || h.cloud.calls[calls] != PriorityConfirm {
		t.Fatalf("calls %v", h.cloud.calls)
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/gateway/ -run Command`
Expected: FAIL — commands are ignored (`bridge [] cloud []`).

- [ ] **Step 3: Implement `commands.go`**

```go
package gateway

import (
	"context"
	"errors"
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/bridge"
	"github.com/t3hk0d3/go-loqed/internal/model"
)

const cloudCommandTimeout = 15 * time.Second

func actionFor(c model.Command) bridge.Action {
	switch c {
	case model.CommandOpen:
		return bridge.ActionOpen
	case model.CommandUnlock:
		return bridge.ActionUnlock
	default:
		return bridge.ActionLock
	}
}

func (s *Supervisor) onCommand(ctx context.Context, m CommandMsg) {
	now := s.d.Now()
	if age := now.Sub(m.At); age > s.t.CommandMaxAge {
		s.log.Warn("dropping a stale command", "command", m.Command, "age", age.Round(time.Second))
		return
	}
	switch s.mode {
	case model.ModeOffline:
		s.log.Warn("command rejected: the lock is offline", "command", m.Command)
		return
	case model.ModeCloud:
		s.commandViaCloud(ctx, m.Command)
		return
	}
	err := s.bridgeCommand(ctx, m.Command)
	if err == nil {
		s.confirmAt = now.Add(s.t.WebhookConfirm)
		return
	}
	if errors.Is(err, loqed.ErrUnauthorized) {
		if s.refreshAndRebuild(ctx, ReasonUnauthorized) && s.bridgeCommand(ctx, m.Command) == nil {
			s.confirmAt = now.Add(s.t.WebhookConfirm)
			return
		}
	} else {
		s.localFailure(ctx, err)
	}
	s.log.Warn("bridge command failed; sending it via the cloud", "command", m.Command, "err", err)
	s.commandViaCloud(ctx, m.Command)
}

func (s *Supervisor) bridgeCommand(ctx context.Context, c model.Command) error {
	if !s.ensureBridge() {
		return errors.New("gateway: no bridge client")
	}
	rc, cancel := s.reqCtx(ctx)
	defer cancel()
	return s.bridge.Command(rc, actionFor(c))
}

func (s *Supervisor) commandViaCloud(ctx context.Context, c model.Command) {
	rc, cancel := context.WithTimeout(ctx, cloudCommandTimeout)
	defer cancel()
	if err := s.d.Cloud.Command(rc, s.id, c.Target()); err != nil {
		s.log.Error("cloud command failed", "command", c, "err", err)
		return
	}
	moving := c.Moving()
	s.state.Lock = &moving
	s.publish()
	if s.mode == model.ModeLocal {
		s.confirmAt = s.d.Now().Add(s.t.WebhookConfirm)
	} else {
		s.confirmAt = s.d.Now().Add(s.t.CloudConfirm)
	}
}
```

- [ ] **Step 4: Route commands in `supervisor.go`**

In `handle`, add the case:

```go
	case CommandMsg:
		s.onCommand(ctx, m)
```

- [ ] **Step 5: Run tests**

Run: `go test ./internal/gateway/ -v -race`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/gateway
git commit -m "gateway: handle lock commands with cloud fallback

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 12: Cloud webhook events and supervisor manager

**Files:**
- Modify: `internal/gateway/cloudmode.go` (replace the `onCloudEvent` stub)
- Create: `internal/gateway/manager.go`, `internal/gateway/cloudevents_test.go`, `internal/gateway/manager_test.go`

**Interfaces:**
- Consumes: `cloud.WebhookEvent` kinds, `recordEvent`, `keyName`, `lastBridgeEvent`, `Timing.EnrichWindow`.
- Produces:
  - `onCloudEvent(ctx, cloud.WebhookEvent)`, `enrichFromCloud(now, cloud.WebhookEvent)`
  - `var gateway.ErrUnknownLock, ErrBusy`
  - `func gateway.NewManager(sups []*Supervisor) *Manager`; `(*Manager).Run(ctx)`, `DeliverBridgeEvent(lockID string, ev bridge.Event) error`, `DeliverCloudEvent(ev cloud.WebhookEvent) error`, `DeliverCommand(lockID string, c model.Command, at time.Time) error`, `BridgeKey(lockID string) ([]byte, bool)`, `Health() map[string]Health`

- [ ] **Step 1: Write failing tests**

`internal/gateway/cloudevents_test.go`:

```go
package gateway

import (
	"testing"
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/bridge"
	"github.com/t3hk0d3/go-loqed/cloud"
	"github.com/t3hk0d3/go-loqed/internal/config"
	"github.com/t3hk0d3/go-loqed/internal/model"
)

func cloudReached(name string) CloudEventMsg {
	return CloudEventMsg{Event: cloud.WebhookEvent{Kind: cloud.KindStateReached, LockID: "lock1", EventType: "STATE_CHANGED_NIGHT_LOCK",
		RequestedState: loqed.BoltNightLock, KeyLocalID: model.Ptr(3), KeyNameUser: name}}
}

func TestCloudEventsDriveStateInCloudMode(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{}, func(d *Deps) { d.CloudWebhooks = true })
	h.start()
	h.toCloud()
	h.cloud.locks[0].BoltState = loqed.BoltDayLock
	h.advance(time.Minute)
	h.send(cloudReached("Front door key"))
	if h.lock() != "LOCKED" {
		t.Fatalf("lock %s", h.lock())
	}
	ev := h.pub.events[len(h.pub.events)-1]
	if ev.EventType != model.EventLocked || ev.KeyName == nil || *ev.KeyName != "Front door key" || *ev.KeyLocalID != 3 {
		t.Fatalf("event %+v", ev)
	}
	calls := len(h.cloud.calls)
	h.advance(time.Minute)
	if len(h.cloud.calls) != calls {
		t.Fatal("with cloud webhooks working, background polling must stop")
	}
}

func TestCloudEventBringsOfflineLockBackToCloud(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.toCloud()
	h.cloud.err = loqed.ErrUnreachable
	for range 3 {
		h.advance(time.Minute)
	}
	h.send(cloudReached(""))
	if h.s.mode != model.ModeCloud || !h.available() {
		t.Fatalf("mode %s", h.s.mode)
	}
}

func TestCloudEventEnrichesMatchingBridgeEvent(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.send(BridgeEventMsg{Event: bridge.StateReachedEvent{EventType: "STATE_CHANGED_NIGHT_LOCK", RequestedState: loqed.BoltNightLock, KeyLocalID: model.Ptr(3)}})
	if h.state().LastKeyName != nil {
		t.Fatal("no name yet")
	}
	h.now = h.now.Add(5 * time.Second)
	h.send(cloudReached("Hallway phone"))
	if n := h.state().LastKeyName; n == nil || *n != "Hallway phone" {
		t.Fatalf("state %+v", h.state())
	}
	if len(h.pub.events) != 1 {
		t.Fatalf("enrichment must not emit a second event: %d", len(h.pub.events))
	}
}

func TestCloudEventOutsideWindowOrUnmatchedIsDropped(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	states := len(h.pub.states)
	h.send(cloudReached("x")) // no bridge event yet
	if len(h.pub.states) != states {
		t.Fatal("unmatched cloud event must be dropped in local mode")
	}
	h.send(BridgeEventMsg{Event: bridge.StateReachedEvent{EventType: "STATE_CHANGED_NIGHT_LOCK", RequestedState: loqed.BoltNightLock}})
	h.now = h.now.Add(31 * time.Second)
	h.send(cloudReached("late"))
	if h.state().LastKeyName != nil {
		t.Fatal("cloud event outside the 30s window must not enrich")
	}
}

func TestConfiguredKeyNameBeatsCloudName(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{KeyNames: config.KeyNames{3: "Alice"}})
	h.start()
	h.toCloud()
	h.send(cloudReached("Front door key"))
	if n := h.state().LastKeyName; n == nil || *n != "Alice" {
		t.Fatalf("state %+v", h.state())
	}
}
```

`internal/gateway/manager_test.go`:

```go
package gateway

import (
	"errors"
	"testing"

	"github.com/t3hk0d3/go-loqed/bridge"
	"github.com/t3hk0d3/go-loqed/cloud"
	"github.com/t3hk0d3/go-loqed/internal/config"
	"github.com/t3hk0d3/go-loqed/internal/model"
)

func TestManagerDispatch(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	m := NewManager([]*Supervisor{h.s})
	if err := m.DeliverBridgeEvent("nope", bridge.OnlineEvent{}); !errors.Is(err, ErrUnknownLock) {
		t.Fatalf("got %v", err)
	}
	if err := m.DeliverCloudEvent(cloud.WebhookEvent{LockID: "lock1"}); err != nil {
		t.Fatal(err)
	}
	if err := m.DeliverCommand("lock1", model.CommandLock, h.now); err != nil {
		t.Fatal(err)
	}
	for range 62 { // queue holds 64; 2 already queued
		_ = m.DeliverCommand("lock1", model.CommandLock, h.now)
	}
	if err := m.DeliverCommand("lock1", model.CommandLock, h.now); !errors.Is(err, ErrBusy) {
		t.Fatalf("got %v", err)
	}
	key, ok := m.BridgeKey("lock1")
	if !ok || string(key) != "bonjour monde" {
		t.Fatalf("key %q %v", key, ok)
	}
	if _, ok := m.Health()["lock1"]; !ok {
		t.Fatal("health missing lock1")
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/gateway/ -run 'Cloud|Manager|Configured'`
Expected: FAIL — `undefined: NewManager`.

- [ ] **Step 3: Replace the `onCloudEvent` stub in `cloudmode.go`**

Add `"strings"` to the imports and replace the stub with:

```go
// onCloudEvent handles a cloud webhook. In local mode it only adds a key
// name to a matching recent bridge event; otherwise it drives state.
func (s *Supervisor) onCloudEvent(ctx context.Context, e cloud.WebhookEvent) {
	now := s.d.Now()
	s.lastCloudEventAt = now
	if s.mode == model.ModeLocal {
		s.enrichFromCloud(now, e)
		return
	}
	if s.mode == model.ModeOffline {
		s.setMode(model.ModeCloud)
		s.nextProbe = now.Add(s.t.Liveness)
	}
	s.nextCloudPoll = now.Add(s.cloudPollInterval(now)) // pushed cloud data replaces polling
	switch e.Kind {
	case cloud.KindStateReached:
		s.state.LockOnline = true
		s.recordEvent(now, e.EventType, e.KeyLocalID, e.KeyNameUser, model.FromStateReached(e.EventType, e.RequestedState), false)
	case cloud.KindGoToState:
		s.recordEvent(now, e.EventType, e.KeyLocalID, e.KeyNameUser, model.FromGoTo(e.GoToState, s.state.Lock), false)
	case cloud.KindSignal:
		if e.BatteryPercentage != nil && *e.BatteryPercentage >= 0 {
			s.state.BatteryPercentage = e.BatteryPercentage
		}
		if e.WifiStrength != nil {
			s.state.WifiStrength = e.WifiStrength
		}
		if e.BLEStrength != nil {
			s.state.BLEStrength = e.BLEStrength
			s.state.LockOnline = *e.BLEStrength != -1
		}
		s.publish()
	case cloud.KindOnline:
		s.state.LockOnline = *e.Online
		s.publish()
	}
}

func (s *Supervisor) enrichFromCloud(now time.Time, e cloud.WebhookEvent) {
	if e.Kind != cloud.KindStateReached && e.Kind != cloud.KindGoToState {
		return
	}
	last := s.lastBridgeEvent
	if last == nil || e.KeyNameUser == "" || s.state.LastKeyName != nil ||
		!strings.EqualFold(last.eventType, e.EventType) || now.Sub(last.at) > s.t.EnrichWindow {
		return
	}
	name := e.KeyNameUser
	s.state.LastKeyName = &name
	s.publish()
}
```

- [ ] **Step 4: Implement `manager.go`**

```go
package gateway

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/t3hk0d3/go-loqed/bridge"
	"github.com/t3hk0d3/go-loqed/cloud"
	"github.com/t3hk0d3/go-loqed/internal/model"
)

var (
	ErrUnknownLock = errors.New("gateway: unknown lock")
	ErrBusy        = errors.New("gateway: lock supervisor is busy")
)

// Manager routes webhooks and commands to lock supervisors.
type Manager struct {
	sups    map[string]*Supervisor
	ordered []*Supervisor
}

func NewManager(sups []*Supervisor) *Manager {
	m := &Manager{sups: make(map[string]*Supervisor, len(sups)), ordered: sups}
	for _, s := range sups {
		m.sups[s.ID()] = s
	}
	return m
}

// Run runs every supervisor until ctx is cancelled.
func (m *Manager) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, s := range m.ordered {
		wg.Go(func() { s.Run(ctx) })
	}
	wg.Wait()
}

func (m *Manager) deliver(lockID string, msg any) error {
	s, ok := m.sups[lockID]
	if !ok {
		return ErrUnknownLock
	}
	if !s.Deliver(msg) {
		return ErrBusy
	}
	return nil
}

func (m *Manager) DeliverBridgeEvent(lockID string, ev bridge.Event) error {
	return m.deliver(lockID, BridgeEventMsg{Event: ev})
}

func (m *Manager) DeliverCloudEvent(ev cloud.WebhookEvent) error {
	return m.deliver(ev.LockID, CloudEventMsg{Event: ev})
}

func (m *Manager) DeliverCommand(lockID string, c model.Command, at time.Time) error {
	return m.deliver(lockID, CommandMsg{Command: c, At: at})
}

func (m *Manager) BridgeKey(lockID string) ([]byte, bool) {
	s, ok := m.sups[lockID]
	if !ok {
		return nil, false
	}
	return s.BridgeKey()
}

func (m *Manager) Health() map[string]Health {
	out := make(map[string]Health, len(m.sups))
	for id, s := range m.sups {
		out[id] = s.Health()
	}
	return out
}
```

- [ ] **Step 5: Run tests**

Run: `go test ./internal/gateway/ -v -race`
Expected: PASS (whole gateway package).

- [ ] **Step 6: Commit**

```bash
git add internal/gateway
git commit -m "gateway: handle cloud webhooks and route messages to supervisors

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 13: Webhook HTTP handler and URLs

**Files:**
- Create: `internal/webhook/handler.go`, `internal/webhook/urls.go`, `internal/webhook/handler_test.go`

**Interfaces:**
- Consumes: `bridge.ParseEvent`, `cloud.ParseWebhook`, `gateway.Health`, `gateway.ErrUnknownLock`, `gateway.ErrBusy`, `gateway.BridgeAddress`.
- Produces:
  - `type webhook.Sink interface{ BridgeKey(lockID string) ([]byte, bool); DeliverBridgeEvent(lockID string, ev bridge.Event) error; DeliverCloudEvent(ev cloud.WebhookEvent) error; Health() map[string]gateway.Health }` (satisfied by `*gateway.Manager`)
  - `type webhook.Options struct{ Sink Sink; CloudSecret string; MQTTConnected func() bool; Now func() time.Time; Log *slog.Logger }` (empty `CloudSecret` = cloud route disabled)
  - `func webhook.NewHandler(o Options) http.Handler` — routes `POST /webhook/{id}`, `POST /cloud/{secret}`, `GET /healthz`
  - `func webhook.PrivateURL(base string, port int, lockID, bridgeIP string) (string, error)`
  - `func webhook.SourceIP(bridgeIP string) (net.IP, error)`
  - `func webhook.CloudURL(publicBase, secret string) string`

- [ ] **Step 1: Write failing tests**

`internal/webhook/handler_test.go`:

```go
package webhook_test

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/t3hk0d3/go-loqed/bridge"
	"github.com/t3hk0d3/go-loqed/cloud"
	"github.com/t3hk0d3/go-loqed/internal/gateway"
	"github.com/t3hk0d3/go-loqed/internal/model"
	"github.com/t3hk0d3/go-loqed/internal/webhook"
)

const secret = "0123456789abcdef0123456789abcdef"

var now = time.Unix(1700000000, 0)

type fakeSink struct {
	bridgeEvents []bridge.Event
	cloudEvents  []cloud.WebhookEvent
	busy         bool
}

func (f *fakeSink) BridgeKey(id string) ([]byte, bool) {
	if id != "lock1" {
		return nil, false
	}
	return []byte("bonjour monde"), true
}

func (f *fakeSink) DeliverBridgeEvent(_ string, ev bridge.Event) error {
	if f.busy {
		return gateway.ErrBusy
	}
	f.bridgeEvents = append(f.bridgeEvents, ev)
	return nil
}

func (f *fakeSink) DeliverCloudEvent(ev cloud.WebhookEvent) error {
	if ev.LockID != "lock1" {
		return gateway.ErrUnknownLock
	}
	f.cloudEvents = append(f.cloudEvents, ev)
	return nil
}

func (f *fakeSink) Health() map[string]gateway.Health {
	return map[string]gateway.Health{"lock1": {Mode: model.ModeLocal, Available: true}}
}

func handler(sink *fakeSink, mqttUp bool, cloudSecret string) http.Handler {
	return webhook.NewHandler(webhook.Options{Sink: sink, CloudSecret: cloudSecret, MQTTConnected: func() bool { return mqttUp },
		Now: func() time.Time { return now }, Log: slog.New(slog.DiscardHandler)})
}

func signed(body string, ts int64) (string, string) {
	h := sha256.New()
	h.Write([]byte(body))
	h.Write(binary.BigEndian.AppendUint64(nil, uint64(ts)))
	h.Write([]byte("bonjour monde"))
	return hex.EncodeToString(h.Sum(nil)), strconv.FormatInt(ts, 10)
}

func post(h http.Handler, path, body string, header map[string]string) int {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	for k, v := range header {
		req.Header[k] = []string{v} // send verbatim like the bridge does
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec.Code
}

const reached = `{"requested_state":"NIGHT_LOCK","event_type":"STATE_CHANGED_NIGHT_LOCK","key_local_id":255}`

func TestBridgeWebhook(t *testing.T) {
	sink := &fakeSink{}
	h := handler(sink, true, "")
	hash, ts := signed(reached, now.Unix())
	if code := post(h, "/webhook/lock1", reached, map[string]string{"HASH": hash, "TIMESTAMP": ts}); code != 200 {
		t.Fatalf("code %d", code)
	}
	if len(sink.bridgeEvents) != 1 {
		t.Fatal("event not delivered")
	}
}

func TestBridgeWebhookErrors(t *testing.T) {
	hash, ts := signed(reached, now.Unix())
	staleHash, staleTS := signed(reached, now.Unix()-60)
	cases := []struct {
		name   string
		path   string
		header map[string]string
		busy   bool
		want   int
	}{
		{"missing headers", "/webhook/lock1", nil, false, 400},
		{"unknown lock", "/webhook/other", map[string]string{"HASH": hash, "TIMESTAMP": ts}, false, 404},
		{"bad hash", "/webhook/lock1", map[string]string{"HASH": strings.Repeat("0", 64), "TIMESTAMP": ts}, false, 401},
		{"stale", "/webhook/lock1", map[string]string{"HASH": staleHash, "TIMESTAMP": staleTS}, false, 401},
		{"busy", "/webhook/lock1", map[string]string{"HASH": hash, "TIMESTAMP": ts}, true, 503},
	}
	for _, c := range cases {
		if code := post(handler(&fakeSink{busy: c.busy}, true, ""), c.path, reached, c.header); code != c.want {
			t.Errorf("%s: got %d want %d", c.name, code, c.want)
		}
	}
}

func TestBridgeWebhookBodyLimit(t *testing.T) {
	big := strings.Repeat("x", 70<<10)
	hash, ts := signed(big, now.Unix())
	if code := post(handler(&fakeSink{}, true, ""), "/webhook/lock1", big, map[string]string{"HASH": hash, "TIMESTAMP": ts}); code != 413 {
		t.Fatalf("code %d", code)
	}
}

func TestCloudWebhook(t *testing.T) {
	sink := &fakeSink{}
	h := handler(sink, true, secret)
	body := `{"requested_state":"DAY_LOCK","event_type":"STATE_CHANGED_LATCH","lock_id":"lock1","key_name_user":"Front door"}`
	if code := post(h, "/cloud/"+secret, body, nil); code != 200 || len(sink.cloudEvents) != 1 {
		t.Fatalf("code %d events %d", code, len(sink.cloudEvents))
	}
	if code := post(h, "/cloud/wrong-secret-0000000000000000", body, nil); code != 404 {
		t.Fatalf("wrong secret: %d", code)
	}
	if code := post(h, "/cloud/"+secret, `{"lock_id":"other","online":1}`, nil); code != 404 {
		t.Fatalf("unknown lock: %d", code)
	}
	if code := post(h, "/cloud/"+secret, `garbage`, nil); code != 400 {
		t.Fatalf("garbage: %d", code)
	}
	if code := post(handler(sink, true, ""), "/cloud/"+secret, body, nil); code != 404 {
		t.Fatalf("cloud route must be off without a secret: %d", code)
	}
}

func TestHealthz(t *testing.T) {
	for _, up := range []bool{true, false} {
		rec := httptest.NewRecorder()
		handler(&fakeSink{}, up, "").ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
		want := 200
		if !up {
			want = 503
		}
		var body map[string]gateway.Health
		if rec.Code != want || json.Unmarshal(rec.Body.Bytes(), &body) != nil || body["lock1"].Mode != model.ModeLocal {
			t.Fatalf("up=%v code %d body %s", up, rec.Code, rec.Body)
		}
	}
}

func TestURLs(t *testing.T) {
	u, err := webhook.PrivateURL("http://gw.lan:8099/", 8099, "lock 1", "192.0.2.10")
	if err != nil || u != "http://gw.lan:8099/webhook/lock%201" {
		t.Fatalf("%q %v", u, err)
	}
	u, err = webhook.PrivateURL("", 8099, "lock1", "127.0.0.1")
	if err != nil || u != "http://127.0.0.1:8099/webhook/lock1" {
		t.Fatalf("%q %v", u, err)
	}
	if got := webhook.CloudURL("https://loqed.example.com/", secret); got != "https://loqed.example.com/cloud/"+secret {
		t.Fatal(got)
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/webhook/`
Expected: FAIL — no non-test files.

- [ ] **Step 3: Implement `handler.go`**

```go
// Package webhook serves bridge and cloud webhooks and the health check.
package webhook

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/bridge"
	"github.com/t3hk0d3/go-loqed/cloud"
	"github.com/t3hk0d3/go-loqed/internal/gateway"
)

const maxBody = 64 << 10

type Sink interface {
	BridgeKey(lockID string) ([]byte, bool)
	DeliverBridgeEvent(lockID string, ev bridge.Event) error
	DeliverCloudEvent(ev cloud.WebhookEvent) error
	Health() map[string]gateway.Health
}

type Options struct {
	Sink          Sink
	CloudSecret   string // empty disables POST /cloud/{secret}
	MQTTConnected func() bool
	Now           func() time.Time
	Log           *slog.Logger
}

func NewHandler(o Options) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /webhook/{id}", o.bridgeWebhook)
	if o.CloudSecret != "" {
		mux.HandleFunc("POST /cloud/{secret}", o.cloudWebhook)
	}
	mux.HandleFunc("GET /healthz", o.healthz)
	return mux
}

func (o Options) bridgeWebhook(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	hash, ts := r.Header.Get("Hash"), r.Header.Get("Timestamp")
	if hash == "" || ts == "" {
		http.Error(w, "missing TIMESTAMP or HASH header", http.StatusBadRequest)
		return
	}
	key, ok := o.Sink.BridgeKey(id)
	if !ok {
		http.NotFound(w, r)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
	if err != nil {
		http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
		return
	}
	ev, err := bridge.ParseEvent(key, body, hash, ts, o.Now())
	switch {
	case errors.Is(err, loqed.ErrStaleTimestamp):
		o.Log.Warn("rejected a bridge webhook with a stale timestamp", "lock_id", id, "err", err)
		http.Error(w, "stale timestamp", http.StatusUnauthorized)
		return
	case errors.Is(err, loqed.ErrBadSignature):
		o.Log.Warn("rejected a bridge webhook with a bad signature", "lock_id", id)
		http.Error(w, "bad signature", http.StatusUnauthorized)
		return
	case err != nil:
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return
	}
	o.deliverResult(w, r, o.Sink.DeliverBridgeEvent(id, ev))
}

func (o Options) cloudWebhook(w http.ResponseWriter, r *http.Request) {
	if subtle.ConstantTimeCompare([]byte(r.PathValue("secret")), []byte(o.CloudSecret)) != 1 {
		http.NotFound(w, r)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
	if err != nil {
		http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
		return
	}
	ev, err := cloud.ParseWebhook(body)
	if err != nil {
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return
	}
	o.deliverResult(w, r, o.Sink.DeliverCloudEvent(ev))
}

func (o Options) deliverResult(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case err == nil:
		w.WriteHeader(http.StatusOK)
	case errors.Is(err, gateway.ErrUnknownLock):
		http.NotFound(w, r)
	default:
		http.Error(w, "busy, retry later", http.StatusServiceUnavailable)
	}
}

func (o Options) healthz(w http.ResponseWriter, _ *http.Request) {
	code := http.StatusOK
	if o.MQTTConnected != nil && !o.MQTTConnected() {
		code = http.StatusServiceUnavailable
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(o.Sink.Health())
}
```

- [ ] **Step 4: Implement `urls.go`**

```go
package webhook

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"

	"github.com/t3hk0d3/go-loqed/internal/gateway"
)

// PrivateURL is the URL the bridge calls for lockID: webhook.private_url
// if set, otherwise http://<this host's address towards the bridge>:<port>.
func PrivateURL(base string, port int, lockID, bridgeIP string) (string, error) {
	path := "/webhook/" + url.PathEscape(lockID)
	if base != "" {
		return strings.TrimRight(base, "/") + path, nil
	}
	ip, err := SourceIP(bridgeIP)
	if err != nil {
		return "", fmt.Errorf("webhook: cannot work out this host's address towards the bridge (set webhook.private_url): %w", err)
	}
	return "http://" + net.JoinHostPort(ip.String(), strconv.Itoa(port)) + path, nil
}

// SourceIP returns the local address the OS would use to reach the bridge.
// Connecting a UDP socket sends no packets.
func SourceIP(bridgeIP string) (net.IP, error) {
	conn, err := net.Dial("udp", gateway.BridgeAddress(bridgeIP))
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return conn.LocalAddr().(*net.UDPAddr).IP, nil
}

func CloudURL(publicBase, secret string) string {
	return strings.TrimRight(publicBase, "/") + "/cloud/" + secret
}
```

- [ ] **Step 5: Run tests**

Run: `go test ./internal/webhook/ -v -race`
Expected: PASS (7 tests).

- [ ] **Step 6: Commit**

```bash
git add internal/webhook
git commit -m "webhook: serve bridge and cloud webhooks and health check

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 14: App wiring, main, end-to-end test

**Files:**
- Create: `internal/app/app.go`, `internal/app/app_test.go`, `cmd/loqed-mqtt/main.go`

**Interfaces:**
- Consumes: every package above.
- Produces:
  - `type app.Options struct{ Config config.Config; Log *slog.Logger; Hostname, Version string; CloudBaseURL, PortalBaseURL string; Now func() time.Time; Ready func(webhookAddr string) }` (empty base URLs = production; `Ready` is a test hook)
  - `func app.Run(ctx context.Context, o Options) error` — returns when ctx is cancelled (nil) or on a startup error.
  - Binary `loqed-mqtt [--config file]`, subcommand `loqed-mqtt healthcheck [--config file]`; `main.version` set via `-ldflags "-X main.version=..."`.

Startup (spec §5.4): open cache → token resolver (+ portal minter when email/password) → hub/budget/refresher → refresh when the cache is missing/corrupt, the token hash differs, an allow-listed lock is missing, or `cache_max_age` passed (fall back to the cache on cloud failure; fail if there is no cache) → select locks → listen → MQTT + discovery (clearing removed locks) → supervisors → HTTP → log the cloud webhook URL once.

- [ ] **Step 1: Write the failing end-to-end test**

`internal/app/app_test.go`:

```go
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
	"testing"
	"time"

	"github.com/t3hk0d3/go-loqed/internal/app"
	"github.com/t3hk0d3/go-loqed/internal/config"
	"github.com/t3hk0d3/go-loqed/internal/model"
	"github.com/t3hk0d3/go-loqed/internal/testutil"
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
		fmt.Fprintf(w, `{"bolt_state":%q,"lock_online":1,"battery_percentage":80,"wifi_strength":70,"ble_strength":40}`, f.bolt)
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

func TestEndToEnd(t *testing.T) {
	broker := testutil.StartBroker(t)
	sub := testutil.Subscribe(t, broker, "#")

	fb := &fakeBridge{bolt: "day_lock"}
	bridgeSrv := httptest.NewServer(fb)
	defer bridgeSrv.Close()
	bridgeHost := strings.TrimPrefix(bridgeSrv.URL, "http://")

	cloudCalls := 0
	cloudSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cloudCalls++
		if r.Header.Get("Authorization") != "Bearer tok" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		fmt.Fprintf(w, `{"data":[{"id":"lock1","name":"Front door","model_name":"LOQED Touch","bolt_state":"day_lock","online":true,
			"bridge_ip":%q,"local_id":1,"key_secret":"SGFsbG8gd2VyZWxk","bridge_key":%q,"bridge_mac_wifi":"aa:bb:cc:dd:ee:ff"}]}`, bridgeHost, bridgeKey)
	}))
	defer cloudSrv.Close()

	// A stale cache built with another token, listing a lock that no longer exists.
	cachePath := filepath.Join(t.TempDir(), "locks.json")
	_ = os.WriteFile(cachePath, []byte(`{"version":1,"token_sha256":"old","locks":[{"id":"gone","name":"Old door"}]}`), 0o600)

	cfg := config.Defaults()
	cfg.CloudToken = "tok"
	cfg.CachePath = cachePath
	cfg.Webhook.Listen = "127.0.0.1:0"
	cfg.MQTT.URL = broker
	cfg.MQTT.ClientID = "gw-e2e"
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan string, 1)
	done := make(chan error, 1)
	go func() {
		done <- app.Run(ctx, app.Options{Config: cfg, Log: slog.New(slog.DiscardHandler), Hostname: "test", Version: "e2e",
			CloudBaseURL: cloudSrv.URL, Ready: func(addr string) { ready <- addr }})
	}()
	select {
	case <-ready:
	case err := <-done:
		t.Fatalf("app exited: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("app did not start")
	}

	sub.WaitFor(t, 10*time.Second, func(m testutil.Message) bool {
		return m.Topic == "homeassistant/device/loqed_lock1/config" && len(m.Payload) > 0
	})
	sub.WaitFor(t, 10*time.Second, func(m testutil.Message) bool {
		return m.Topic == "homeassistant/device/loqed_gone/config" && len(m.Payload) == 0
	})
	sub.WaitFor(t, 10*time.Second, func(m testutil.Message) bool {
		var s model.State
		return m.Topic == "loqed/lock1/state" && json.Unmarshal(m.Payload, &s) == nil && s.Lock != nil && *s.Lock == model.Unlocked && s.Mode == model.ModeLocal
	})
	if cloudCalls != 1 {
		t.Fatalf("expected exactly one cloud call, got %d", cloudCalls)
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
	req.Header["TIMESTAMP"] = []string{strconv.FormatInt(ts, 10)}
	req.Header["HASH"] = []string{hex.EncodeToString(h.Sum(nil))}
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("webhook post: %v %v", resp, err)
	}
	resp.Body.Close()

	sub.WaitFor(t, 10*time.Second, func(m testutil.Message) bool {
		var s model.State
		return m.Topic == "loqed/lock1/state" && json.Unmarshal(m.Payload, &s) == nil && s.Lock != nil && *s.Lock == model.Locked
	})
	sub.WaitFor(t, 10*time.Second, func(m testutil.Message) bool {
		var e model.Event
		return m.Topic == "loqed/lock1/event" && json.Unmarshal(m.Payload, &e) == nil && e.EventType == model.EventLocked
	})

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

func TestFailsWithoutCacheOrCloud(t *testing.T) {
	cfg := config.Defaults()
	cfg.CloudToken = "tok"
	cfg.CachePath = filepath.Join(t.TempDir(), "locks.json")
	cfg.Webhook.Listen = "127.0.0.1:0"
	cfg.MQTT.URL = "tcp://127.0.0.1:1"
	err := app.Run(context.Background(), app.Options{Config: cfg, Log: slog.New(slog.DiscardHandler), Hostname: "t", CloudBaseURL: "http://127.0.0.1:1"})
	if err == nil || !strings.Contains(err.Error(), "no credential cache") {
		t.Fatalf("got %v", err)
	}
}

func TestFailsWithoutAnyCredentials(t *testing.T) {
	cfg := config.Defaults()
	cfg.CachePath = filepath.Join(t.TempDir(), "locks.json")
	err := app.Run(context.Background(), app.Options{Config: cfg, Log: slog.New(slog.DiscardHandler), Hostname: "t"})
	if err == nil || !strings.Contains(err.Error(), "cloud_token") {
		t.Fatalf("got %v", err)
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/app/`
Expected: FAIL — no non-test files.

- [ ] **Step 3: Implement `internal/app/app.go`**

```go
// Package app wires loqed-mqtt together.
package app

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/t3hk0d3/go-loqed/bridge"
	"github.com/t3hk0d3/go-loqed/cloud"
	"github.com/t3hk0d3/go-loqed/cloud/portal"
	"github.com/t3hk0d3/go-loqed/internal/auth"
	"github.com/t3hk0d3/go-loqed/internal/config"
	"github.com/t3hk0d3/go-loqed/internal/gateway"
	"github.com/t3hk0d3/go-loqed/internal/hass"
	"github.com/t3hk0d3/go-loqed/internal/store"
	"github.com/t3hk0d3/go-loqed/internal/webhook"
)

type Options struct {
	Config        config.Config
	Log           *slog.Logger
	Hostname      string
	Version       string
	CloudBaseURL  string
	PortalBaseURL string
	Now           func() time.Time
	Ready         func(webhookAddr string)
}

func Run(ctx context.Context, o Options) error {
	cfg, log := o.Config, o.Log
	now := o.Now
	if now == nil {
		now = time.Now
	}
	cloudBase, portalBase := o.CloudBaseURL, o.PortalBaseURL
	if cloudBase == "" {
		cloudBase = cloud.DefaultBaseURL
	}
	if portalBase == "" {
		portalBase = portal.DefaultBaseURL
	}

	st, status, err := store.Open(cfg.CachePath)
	if err != nil {
		return err
	}
	if status == store.StatusCorrupt {
		log.Warn("the credential cache is unreadable; rebuilding it from the cloud", "path", cfg.CachePath)
	}
	before := st.Snapshot()

	var minter auth.Minter
	if cfg.CloudEmail != "" && cfg.CloudPassword != "" {
		minter = auth.NewPortalMinter(portal.New(portal.WithBaseURL(portalBase)), cfg.CloudEmail, cfg.CloudPassword, auth.TokenName(o.Hostname))
	}
	resolver := auth.NewResolver(cfg.CloudToken, minter, st, now, log)
	hub := gateway.NewCloudHub(gateway.NewBudget(cfg.CloudBudget, 12*time.Hour, now), resolver,
		func(tok string) gateway.CloudAPI { return cloud.New(tok, cloud.WithBaseURL(cloudBase)) }, now, log)
	refresher := gateway.NewRefresher(hub, st, now)

	if err := initialRefresh(ctx, cfg, st, status, resolver, refresher, now, log); err != nil {
		return err
	}
	selected, missing := gateway.Select(st.Snapshot().Locks, cfg.Locks)
	for _, m := range missing {
		log.Warn("a lock from the locks allow-list is not on the account", "lock", m)
	}
	if len(selected) == 0 {
		return errors.New("no locks to manage: the account has no locks, or the locks allow-list matches none")
	}

	cloudSecret := ""
	if cfg.Webhook.PublicURL != "" {
		if cloudSecret, err = ensureCloudSecret(cfg, st); err != nil {
			return err
		}
	}

	ln, err := net.Listen("tcp", cfg.Webhook.Listen)
	if err != nil {
		return fmt.Errorf("listening for webhooks on %s: %w", cfg.Webhook.Listen, err)
	}
	port := ln.Addr().(*net.TCPAddr).Port

	mq := hass.NewClient(hass.ClientConfig{
		URL: cfg.MQTT.URL, Username: cfg.MQTT.Username, Password: cfg.MQTT.Password, ClientID: cfg.MQTT.ClientID,
		Topics:    hass.Topics{Base: cfg.MQTT.BaseTopic, DiscoveryPrefix: cfg.HomeAssistant.DiscoveryPrefix},
		HAEnabled: cfg.HomeAssistant.Enabled, Version: o.Version,
	}, log)
	infos := make([]hass.LockInfo, 0, len(selected))
	for _, r := range selected {
		infos = append(infos, hass.LockInfo{ID: r.ID, Name: r.Name, Model: r.ModelName, MacWifi: r.BridgeMacWifi})
	}
	mq.SetLocks(infos, removedIDs(before.Locks, selected))
	mq.Start()
	defer mq.Close()

	deps := gateway.Deps{
		Publisher: mq, Cloud: hub, Refresh: refresher.Refresh,
		NewBridge: func(rec store.LockRecord) (gateway.BridgeAPI, error) {
			if rec.LocalID == nil {
				return nil, errors.New("lock has no local key id")
			}
			c, err := bridge.New(rec.BridgeIP, bridge.Credentials{BridgeKey: rec.BridgeKey, KeySecret: rec.KeySecret, LocalKeyID: uint8(*rec.LocalID)}, bridge.WithClock(now))
			if err != nil {
				return nil, err
			}
			return c, nil
		},
		Probe: gateway.TCPProbe,
		WebhookURL: func(rec store.LockRecord) (string, error) {
			return webhook.PrivateURL(cfg.Webhook.PrivateURL, port, rec.ID, rec.BridgeIP)
		},
		CloudWebhooks: cfg.Webhook.PublicURL != "",
		Now:           now,
		Log:           log,
	}
	timing := gateway.DefaultTiming(cfg.LivenessInterval.D(), cfg.ReconcileInterval.D())
	sups := make([]*gateway.Supervisor, 0, len(selected))
	for _, r := range selected {
		sups = append(sups, gateway.NewSupervisor(r, gateway.SettingFor(cfg.LockSettings, r), deps, timing))
	}
	manager := gateway.NewManager(sups)

	srv := &http.Server{
		Handler: webhook.NewHandler(webhook.Options{Sink: manager, CloudSecret: cloudSecret, MQTTConnected: mq.Connected, Now: now, Log: log}),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("webhook server stopped", "err", err)
		}
	}()
	if cloudSecret != "" {
		log.Info("register this URL as the webhook in the API section of app.loqed.com", "url", webhook.CloudURL(cfg.Webhook.PublicURL, cloudSecret))
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case c := <-mq.Commands():
				if err := manager.DeliverCommand(c.LockID, c.Command, c.At); err != nil {
					log.Warn("command not delivered", "lock_id", c.LockID, "err", err)
				}
			}
		}
	}()
	if o.Ready != nil {
		o.Ready(ln.Addr().String())
	}

	manager.Run(ctx)
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}

func initialRefresh(ctx context.Context, cfg config.Config, st *store.Store, status store.Status,
	resolver *auth.Resolver, refresher *gateway.Refresher, now func() time.Time, log *slog.Logger) error {
	snap := st.Snapshot()
	tok, err := resolver.Token(ctx)
	if errors.Is(err, auth.ErrNoToken) {
		return err
	}
	if err != nil {
		if len(snap.Locks) > 0 {
			log.Warn("no usable LOQED token; starting from the credential cache", "err", err)
			return nil
		}
		return fmt.Errorf("cannot start: no credential cache and no usable LOQED token: %w", err)
	}
	need := status != store.StatusLoaded ||
		snap.TokenSHA256 != store.TokenHash(tok) ||
		missingFromCache(snap, cfg.Locks) ||
		(cfg.CacheMaxAge > 0 && now().Sub(snap.FetchedAt) > cfg.CacheMaxAge.D())
	if !need {
		return nil
	}
	if _, err := refresher.RefreshAll(ctx); err != nil {
		if len(snap.Locks) > 0 {
			log.Warn("cloud refresh failed; starting from the credential cache", "err", err)
			return nil
		}
		return fmt.Errorf("cannot start: no credential cache and the LOQED cloud is unavailable: %w", err)
	}
	return nil
}

func missingFromCache(c store.Cache, allow []string) bool {
	for _, a := range allow {
		if _, ok := c.Find(a); !ok {
			return true
		}
	}
	return false
}

func removedIDs(before, selected []store.LockRecord) []string {
	keep := make(map[string]bool, len(selected))
	for _, r := range selected {
		keep[r.ID] = true
	}
	var out []string
	for _, r := range before {
		if !keep[r.ID] {
			out = append(out, r.ID)
		}
	}
	return out
}

func ensureCloudSecret(cfg config.Config, st *store.Store) (string, error) {
	if cfg.Webhook.CloudSecret != "" {
		return cfg.Webhook.CloudSecret, nil
	}
	if s := st.Snapshot().CloudSecret; s != "" {
		return s, nil
	}
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	secret := base64.RawURLEncoding.EncodeToString(b)
	if err := st.Update(func(c *store.Cache) { c.CloudSecret = secret }); err != nil {
		return "", fmt.Errorf("saving the cloud webhook secret: %w", err)
	}
	return secret, nil
}
```

- [ ] **Step 4: Implement `cmd/loqed-mqtt/main.go`**

```go
// Command loqed-mqtt bridges LOQED smart locks to MQTT and Home Assistant.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/t3hk0d3/go-loqed/internal/app"
	"github.com/t3hk0d3/go-loqed/internal/config"
)

var version = "dev"

const optionsFile = "/data/options.json"

func main() { os.Exit(run(os.Args[1:])) }

func run(args []string) int {
	if len(args) > 0 && args[0] == "healthcheck" {
		return healthcheck(args[1:])
	}
	fs := flag.NewFlagSet("loqed-mqtt", flag.ContinueOnError)
	configPath := fs.String("config", "", "path to a YAML config file")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	cfg, err := loadConfig(*configPath)
	if err == nil {
		err = cfg.Validate()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "loqed-mqtt: invalid configuration:", err)
		return 2
	}
	var level slog.Level
	_ = level.UnmarshalText([]byte(cfg.LogLevel))
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := config.ResolveMQTT(ctx, &cfg, os.Getenv("SUPERVISOR_TOKEN"), config.SupervisorURL, &http.Client{Timeout: 10 * time.Second}); err != nil {
		log.Error("cannot determine the MQTT broker", "err", err)
		return 1
	}
	host, _ := os.Hostname()
	log.Info("starting loqed-mqtt", "version", version)
	if err := app.Run(ctx, app.Options{Config: cfg, Log: log, Hostname: host, Version: version}); err != nil {
		log.Error("loqed-mqtt stopped", "err", err)
		return 1
	}
	return 0
}

func loadConfig(path string) (config.Config, error) {
	return config.Load(config.Sources{OptionsFile: optionsFile, ConfigFile: path, Environ: os.Environ()})
}

// healthcheck is used by Docker HEALTHCHECK: distroless has no shell or curl.
func healthcheck(args []string) int {
	fs := flag.NewFlagSet("healthcheck", flag.ContinueOnError)
	configPath := fs.String("config", "", "path to a YAML config file")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	cfg, err := loadConfig(*configPath)
	if err != nil {
		return 1
	}
	_, port, err := net.SplitHostPort(cfg.Webhook.Listen)
	if err != nil {
		return 1
	}
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Get("http://127.0.0.1:" + port + "/healthz")
	if err != nil {
		return 1
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}
```

- [ ] **Step 5: Run all tests and build**

Run: `go test -race ./... && go build -o /dev/null ./cmd/loqed-mqtt`
Expected: PASS; build succeeds.

- [ ] **Step 6: Commit**

```bash
git add internal/app cmd
git commit -m "app: wire gateway, add loqed-mqtt command and end-to-end test

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 15: Docker image, Home Assistant add-on, CI, docs

**Files:**
- Create: `Dockerfile`, `.dockerignore`, `docker-compose.yml`, `addon/config.yaml`, `addon/build.yaml`, `addon/Dockerfile`, `addon/DOCS.md`, `addon/translations/en.yaml`, `repository.yaml`, `README.md`, `.golangci.yml`, `.github/workflows/ci.yml`, `.github/workflows/release.yml`

**Interfaces:**
- Consumes: `cmd/loqed-mqtt` (`healthcheck` subcommand, `main.version`), config keys from Task 1.
- Produces: image `ghcr.io/t3hk0d3/loqed-mqtt:<version>` for `linux/amd64`, `linux/arm64`, `linux/arm/v7`; an add-on repository installable from `https://github.com/t3hk0d3/go-loqed`.

Note on users: the image runs as the distroless `nonroot` user. The HA Supervisor mounts `/data` owned by root, so the add-on is a one-line local build on top of the image that switches back to root.

- [ ] **Step 1: Dockerfile and compose**

`Dockerfile`:

```dockerfile
# syntax=docker/dockerfile:1
FROM --platform=$BUILDPLATFORM golang:1.27-alpine AS build
ARG TARGETOS TARGETARCH TARGETVARIANT
ARG VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH GOARM=${TARGETVARIANT#v} \
    go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/loqed-mqtt ./cmd/loqed-mqtt

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/loqed-mqtt /loqed-mqtt
EXPOSE 8099
VOLUME /data
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s CMD ["/loqed-mqtt", "healthcheck"]
ENTRYPOINT ["/loqed-mqtt"]
```

`.dockerignore`:

```
.git
.artifact
dist
docs
addon
*.md
```

`docker-compose.yml`:

```yaml
services:
  loqed-mqtt:
    image: ghcr.io/t3hk0d3/loqed-mqtt:latest
    restart: unless-stopped
    # Host networking makes the auto-detected webhook URL the real LAN address
    # the bridge can reach.
    network_mode: host
    user: "65532:65532"
    volumes:
      - ./data:/data
    environment:
      LOQED_CLOUD_TOKEN: "paste-your-personal-access-token"
      # or: LOQED_CLOUD_EMAIL / LOQED_CLOUD_PASSWORD
      LOQED_MQTT__URL: "tcp://192.168.1.10:1883"
      LOQED_MQTT__USERNAME: "loqed"
      LOQED_MQTT__PASSWORD: "change-me"
```

Run (host with Docker): `mkdir -p data && sudo chown 65532:65532 data` is documented in README; then `docker build -t loqed-mqtt:dev . && docker run --rm loqed-mqtt:dev healthcheck; echo $?`
Expected: image builds; healthcheck prints nothing and exits `1` (no gateway running), which proves the binary runs in distroless.

- [ ] **Step 2: Add-on files**

`repository.yaml`:

```yaml
name: LOQED MQTT add-ons
url: https://github.com/t3hk0d3/go-loqed
maintainer: t3hk0d3
```

`addon/config.yaml`:

```yaml
name: LOQED MQTT Gateway
version: "0.1.0"
slug: loqed_mqtt
description: Local-first LOQED smart lock gateway for MQTT and Home Assistant
url: https://github.com/t3hk0d3/go-loqed
arch: [amd64, aarch64, armv7]
init: false
host_network: true
services:
  - mqtt:need
watchdog: http://[HOST]:[PORT:8099]/healthz
ports:
  8099/tcp: 8099
options:
  cloud_token: ""
  locks: []
  lock_settings: []
  homeassistant:
    enabled: true
    discovery_prefix: homeassistant
  log_level: info
schema:
  cloud_token: password?
  cloud_email: email?
  cloud_password: password?
  locks: [str]
  lock_settings:
    - lock: str
      bridge_ip: str?
      bridge_key: password?
      key_secret: password?
      local_id: int(0,255)?
      key_names: [str]
  cache_max_age: str?
  reconcile_interval: str?
  liveness_interval: str?
  cloud_budget: int(1,12)?
  webhook:
    listen: str?
    private_url: url?
    public_url: url?
    cloud_secret: password?
  mqtt:
    url: str?
    username: str?
    password: password?
    client_id: str?
    base_topic: str?
  homeassistant:
    enabled: bool?
    discovery_prefix: str?
  log_level: list(debug|info|warn|error)?
```

`addon/build.yaml` (keep the tag equal to `version` in `config.yaml`):

```yaml
build_from:
  amd64: ghcr.io/t3hk0d3/loqed-mqtt:0.1.0
  aarch64: ghcr.io/t3hk0d3/loqed-mqtt:0.1.0
  armv7: ghcr.io/t3hk0d3/loqed-mqtt:0.1.0
```

`addon/Dockerfile`:

```dockerfile
ARG BUILD_FROM
FROM ${BUILD_FROM}
# The Supervisor mounts /data owned by root.
USER root
```

`addon/translations/en.yaml`:

```yaml
configuration:
  cloud_token:
    name: Personal access token
    description: Create one at https://integrations.loqed.com/personal-access-tokens. Leave empty to use email and password instead.
  cloud_email:
    name: LOQED email
    description: Used only to create a personal access token automatically.
  cloud_password:
    name: LOQED password
    description: Can be removed after the first successful start.
  locks:
    name: Locks
    description: Names or ids of the locks to expose. Empty exposes every lock on the account.
  lock_settings:
    name: Lock settings
    description: Per-lock overrides. key_names entries look like "1=Alice".
  webhook:
    name: Webhooks
    description: private_url is the address the bridge calls (auto-detected when empty). public_url enables cloud webhooks.
  mqtt:
    name: MQTT
    description: Leave empty to use the Mosquitto add-on.
  log_level:
    name: Log level
```

`addon/DOCS.md`:

```markdown
# LOQED MQTT Gateway

Exposes every LOQED lock on your account to Home Assistant through MQTT.
The gateway talks to your LOQED Bridge on the local network and falls back
to the LOQED cloud when the bridge is unreachable.

## Setup

1. Install and start the Mosquitto broker add-on.
2. Create a personal access token at
   https://integrations.loqed.com/personal-access-tokens and paste it into
   **Personal access token**. Alternatively, enter your LOQED email and
   password and the add-on creates a token named `loqed-mqtt (<hostname>)`.
3. Start the add-on. Each lock appears as a device with a lock, battery and
   signal sensors, a connection mode sensor, a last change reason sensor and
   a lock event entity.

## Things to know

- **Time must be correct.** The bridge signs webhooks with a timestamp that
  must be within 10 seconds of this host's clock.
- **Lock events are best-effort.** The bridge occasionally loses a webhook.
  Use the lock entity, not the event entity, for automations that depend on
  whether the door is locked.
- **Cloud limits.** LOQED blocks accounts that read lock status more than 12
  times in 12 hours. The gateway keeps cloud reads under `cloud_budget`
  (default 10), so in cloud mode without cloud webhooks the lock state can be
  more than an hour old. The `state_stale` attribute shows this.
- **Cloud webhooks (optional).** Set `webhook.public_url` to an address that
  reaches this add-on from the internet (for example through a reverse
  proxy that forwards only the `/cloud/` path). The add-on log shows the
  full URL once at startup; register it in the API section of
  https://app.loqed.com. Cloud events then update the lock in cloud mode
  immediately and add key names to local events.
```

- [ ] **Step 3: README, lint config, CI**

`README.md`:

```markdown
# go-loqed

- **GoLoqed** (`bridge`, `cloud`, `cloud/portal`): a Go client for the LOQED
  local Bridge API, the cloud Lock API and the Integrations portal.
- **loqed-mqtt** (`cmd/loqed-mqtt`): a local-first MQTT gateway for LOQED
  locks with Home Assistant discovery and automatic cloud fallback.

## Running loqed-mqtt

Home Assistant OS: add this repository in *Settings → Add-ons → Add-on
store → Repositories* and install **LOQED MQTT Gateway**.

Docker: see `docker-compose.yml`. Create the data directory first:
`mkdir -p data && sudo chown 65532:65532 data`.

Configuration comes from `/data/options.json`, an optional YAML file
(`--config`), and `LOQED_*` environment variables (nested keys use `__`,
for example `LOQED_MQTT__BASE_TOPIC`), in increasing order of precedence.
See `docs/superpowers/specs/2026-10-04-loqed-mqtt-gateway-design.md` for
every setting.

## MQTT topics

| Topic | Retained | Payload |
|---|---|---|
| `loqed/status` | yes | `online` / `offline` |
| `loqed/<id>/availability` | yes | `online` / `offline` |
| `loqed/<id>/state` | yes | JSON state document |
| `loqed/<id>/event` | no | JSON lock event |
| `loqed/<id>/command` | – | `LOCK`, `UNLOCK` or `OPEN` |

## Development

    go test -race ./...
    python3 testdata/gen_vectors.py   # regenerate signing golden vectors
```

`.golangci.yml`:

```yaml
version: "2"
linters:
  default: standard
  enable:
    - errorlint
    - gosec
    - misspell
    - unconvert
```

`.github/workflows/ci.yml`:

```yaml
name: ci
on:
  push:
    branches: [main]
  pull_request:
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version-file: go.mod
      - run: go vet ./...
      - run: go test -race ./...
      - uses: golangci/golangci-lint-action@v8
        with:
          version: latest
  image:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: docker/setup-qemu-action@v3
      - uses: docker/setup-buildx-action@v3
      - uses: docker/build-push-action@v6
        with:
          context: .
          platforms: linux/amd64,linux/arm64,linux/arm/v7
          push: false
```

`.github/workflows/release.yml`:

```yaml
name: release
on:
  push:
    tags: ["v*"]
permissions:
  contents: read
  packages: write
jobs:
  image:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: docker/setup-qemu-action@v3
      - uses: docker/setup-buildx-action@v3
      - uses: docker/login-action@v3
        with:
          registry: ghcr.io
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}
      - id: version
        run: echo "version=${GITHUB_REF_NAME#v}" >> "$GITHUB_OUTPUT"
      - uses: docker/build-push-action@v6
        with:
          context: .
          platforms: linux/amd64,linux/arm64,linux/arm/v7
          push: true
          build-args: VERSION=${{ steps.version.outputs.version }}
          tags: |
            ghcr.io/t3hk0d3/loqed-mqtt:${{ steps.version.outputs.version }}
            ghcr.io/t3hk0d3/loqed-mqtt:latest
```

Releasing: bump `addon/config.yaml` `version` and the three tags in `addon/build.yaml`, commit, then tag `v<version>`.

- [ ] **Step 4: Verify**

Run:

```bash
go vet ./... && go test -race ./...
docker build --build-arg VERSION=0.1.0-dev -t loqed-mqtt:dev .
docker run --rm loqed-mqtt:dev --config /nonexistent.yaml; echo "exit=$?"
python3 -c "import yaml,sys; [yaml.safe_load(open(f)) for f in sys.argv[1:]]" addon/config.yaml addon/build.yaml addon/translations/en.yaml repository.yaml docker-compose.yml .github/workflows/ci.yml .github/workflows/release.yml
```

Expected: tests pass; image builds; the container prints `loqed-mqtt: invalid configuration: config: open /nonexistent.yaml: no such file or directory` and `exit=2`; YAML files parse (if PyYAML is missing, `pip install pyyaml` in a venv or skip this line).

- [ ] **Step 5: Commit**

```bash
git add Dockerfile .dockerignore docker-compose.yml addon repository.yaml README.md .golangci.yml .github
git commit -m "Add Docker image, Home Assistant add-on, CI and docs

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Spec coverage

| Spec section | Task |
|---|---|
| §2.4 V1–V7 manual verification | after Task 15, against real hardware (not automated) |
| §4 library | library plan |
| §5.1 configuration | 1 |
| §5.2 cloud authentication | 3, 14 |
| §5.3 credential cache + refresh rules 1–6 | 2, 8, 9, 10, 14 |
| §5.4 startup | 14 |
| §5.5 supervisor/failover/commands/availability | 9, 10, 11 |
| §5.6 cloud budget | 7, 10 |
| §5.7 state + event mapping, key names, enrichment | 4, 9, 12 |
| §6 MQTT + discovery | 5, 6 |
| §7 webhook listener | 13 |
| §8 logging/error handling | 1, 9–14 (log calls; secrets never passed to loggers) |
| §9 packaging | 15 |
| §10 testing | every task; end-to-end in 14 |

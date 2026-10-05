# loqed-mqtt Gateway Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `loqed-mqtt`: a gateway that exposes every LOQED lock on an account to MQTT / Home Assistant, local-bridge first with automatic cloud fallback, shipped as a Docker image and a Home Assistant add-on.

**Architecture:** `cmd/loqed-mqtt` only wires `internal/app`. `internal/config` loads settings, `internal/store` caches cloud credentials (and the cloud request budget), `internal/auth` resolves/mints cloud tokens, `internal/model` holds the state document and the LOQED→HA mapping, `internal/hass` owns MQTT and discovery, `internal/gateway` owns the per-lock failover state machine and the cloud request budget, `internal/webhook` serves bridge/cloud webhooks and `/healthz`. Each lock runs in one supervisor goroutine; all inputs reach it over a channel, and its handlers are plain methods so tests drive them with a fake clock.

**Tech Stack:** Go 1.27, GoLoqed (this module), `gopkg.in/yaml.v3` v3.0.1, `github.com/eclipse/paho.mqtt.golang` v1.5.1, `github.com/mochi-mqtt/server/v2` v2.7.9 (tests only), Docker buildx, GitHub Actions, golangci-lint v2.14.0.

**Spec:** `docs/superpowers/specs/2026-10-04-loqed-mqtt-gateway-design.md` (rev 2)

**Prerequisite:** `docs/superpowers/plans/2026-10-04-goloqed-library.md` is fully implemented (packages `loqed`, `bridge`, `cloud`, `cloud/portal`, `internal/transport`, and `.golangci.yml`).

**Revision 2** (after an adversarial review of revision 1): commands fall back to the cloud only when the bridge provably did not receive them; retained MQTT commands are ignored; every actuation runs under the command's 10 s deadline; confirmation polls never use pre-command cached data; the cloud budget and rate-limit block survive restarts and count cloud commands; refreshes merge per lock and back off exponentially; offline detection uses an unbudgeted TCP probe; state goes stale when data stops arriving; the add-on uses a prebuilt image. Every code block below was compiled and tested (`go test -race`, golangci-lint) before it was pasted into this plan.

## Global Constraints

- Module path `github.com/t3hk0d3/go-loqed`, `go 1.27`.
- Config precedence: environment (`LOQED_` prefix, nesting `__`) > YAML file (`--config`) > add-on `/data/options.json` (decoded as JSON).
- Defaults: `cache_path=/data/locks.json`, `cache_max_age=0`, `reconcile_interval=24h`, `liveness_interval=60s`, `cloud_budget=10` (max 12), `webhook.listen=":8099"`, `mqtt.client_id=loqed-mqtt`, `mqtt.base_topic=loqed`, `homeassistant.enabled=true`, `homeassistant.discovery_prefix=homeassistant`, `log_level=info`, `log_format=text`.
- Cache file written atomically with mode `0600`; it also holds `install_id`, `published_ids` and the budget window.
- Cloud budget: at most `cloud_budget` cloud calls per rolling 12 h, account-wide, persisted; confirmations may use all, refreshes leave 1, background polls leave 2 and are spaced `12h/cloud_budget`; cloud commands are never refused but are recorded; on rate limit suspend cloud reads 12 h.
- Failover: 3 consecutive failures (TCP probe, bridge HTTP, cloud probe and cloud API failures counted separately), 5 s request timeout, offline retry every 5 min forever (TCP probes only), unknown-state status recheck at most every 10 min, command max age 10 s (absolute deadline for every actuation), webhook confirm 10 s, webhook registration retry 10 min, cloud confirm 5 s, stale grace 10 min, cloud enrichment window 30 s, refresh backoff 5 min doubling to 6 h per lock+reason, re-mint at most once per hour (persisted).
- A command is resent via the cloud **only** after `loqed.ErrUnreachable` (or an unusable bridge client) or `loqed.ErrUnauthorized`; never after `ErrNoResponse` or any other error.
- Bridge addressed by IP only (never hostnames/mDNS).
- MQTT topics: `<base>/status` (retained, LWT `offline`), `<base>/<id>/availability` (retained), `<base>/<id>/state` (retained JSON), `<base>/<id>/event` (**not** retained), `<base>/<id>/command` (subscribe QoS 1; retained messages ignored). Discovery `<prefix>/device/loqed_<id>/config`.
- Never log tokens, passwords, keys, signed commands, URLs with credentials, full cloud responses, cloud webhook bodies; the cloud webhook URL is logged once at startup only.
- Minted token name: `loqed-mqtt <install-id>`.
- Code is `gofmt`-clean and passes `go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.14.0 run ./...`.
- Commit messages end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

- A slow bridge that received an `OPEN` must never cause a second `OPEN` through the cloud (Task 11 no-response tests; library `ErrNoResponse`).
- A retained `OPEN` on the command topic must never unlatch the door on reconnect (Task 6 retained-command test).
- Crash/restart loops must not exceed LOQED's 12-calls-per-12-h account limit (Task 7 persistence test, Task 14 typo-restart test).
- Home Assistant must never show a definite state that is older than reality without `state_stale` (Tasks 10 and 12: stale on entering cloud, deferred polls, older polls ignored, confirm polls skip cached data).
- Add-on options must load on a fresh Supervisor install (Task 15 options/schema test), and a lock removed from the account must disappear from HA without a restart (Tasks 12 and 14).

---

## File Structure

```
internal/config/config.go        Config types, defaults, Validate, YAML forms of lock_settings/key_names
internal/config/load.go          Load(Sources): options.json (JSON) < YAML < env
internal/config/supervisor.go    ResolveMQTT via Supervisor services API (+ loopback fallback)
internal/store/store.go          credential cache (Store, Cache, LockRecord, BudgetState, Merge)
internal/auth/auth.go            token Resolver, PortalMinter
internal/model/model.go          State, Event, Command, enums, command failure classes
internal/model/mapping.go        LOQED events → transitions, sources, key ids
internal/hass/topics.go          topic layout, TopicID
internal/hass/discovery.go       device discovery payload
internal/hass/client.go          paho client: publish, commands, birth handling, removal
internal/testutil/mqtt.go        in-process broker + subscriber for tests
internal/gateway/budget.go       persisted cloud request budget
internal/gateway/cloudhub.go     coalesced, budgeted, re-authenticating cloud access
internal/gateway/refresher.go    credential refresh (merge, backoff) into the store
internal/gateway/records.go      allow-list + lock_settings application
internal/gateway/supervisor.go   Supervisor core, deps, timing, publishing, confirmations
internal/gateway/local.go        local mode, bridge events, liveness, reconcile, webhook registration
internal/gateway/cloudmode.go    cloud/offline modes, cloud probes and polling, freshness
internal/gateway/commands.go     command handling, deadlines, fallback rules
internal/gateway/cloudevents.go  cloud webhook events, enrichment
internal/gateway/manager.go      dispatch to supervisors, runtime removal, health
internal/webhook/handler.go      HTTP routes
internal/webhook/urls.go         private/public URL building, source IP
internal/app/app.go              startup wiring
cmd/loqed-mqtt/main.go           flags, logger, signals, healthcheck subcommand
Dockerfile, .dockerignore, docker-compose.yml
addon/config.yaml, addon/DOCS.md, addon/translations/en.yaml
repository.yaml, README.md
.github/workflows/ci.yml, .github/workflows/release.yml
```

---

### Task 1: Configuration

**Files:**
- Create: `internal/config/config.go`, `internal/config/load.go`, `internal/config/supervisor.go`, `internal/config/config_test.go`

**Interfaces:**
- Produces:
  - `type config.Duration time.Duration` with `UnmarshalYAML` and `func (Duration) D() time.Duration`
  - `type config.Config struct{ CloudToken, CloudEmail, CloudPassword string; Locks []string; LockSettings LockSettingsMap; CachePath string; CacheMaxAge, ReconcileInterval, LivenessInterval Duration; CloudBudget int; Webhook Webhook; MQTT MQTT; HomeAssistant HomeAssistant; LogLevel, LogFormat string }`
  - `type config.Webhook struct{ Listen, PrivateURL, PublicURL, CloudSecret string }`
  - `type config.MQTT struct{ URL, Username, Password, ClientID, BaseTopic string }`
  - `type config.HomeAssistant struct{ Enabled bool; DiscoveryPrefix string }`
  - `type config.LockSetting struct{ BridgeIP, BridgeKey, KeySecret string; LocalID *int; KeyNames KeyNames }`
  - `type config.LockSettingsMap map[string]LockSetting` (mapping, or list of entries with a `lock` field), `type config.KeyNames map[int]string` (string `"1=Alice,3=Bob"`, mapping with string or int keys, list of `"1=Alice"`, or list of `{id, name}`)
  - `func config.Defaults() Config`, `type config.Sources struct{ OptionsFile, ConfigFile string; Environ []string }`, `func config.Load(Sources) (Config, error)`
  - `func (Config) Validate() error`, `func (Config) HasCloudCredentials() bool` (token or e-mail), `func (Config) CanMint() bool` (e-mail and password)
  - `const config.SupervisorURL = "http://supervisor"`, `const config.DefaultMQTTURL = "tcp://localhost:1883"`
  - `type config.LookupHost func(ctx, host string) ([]string, error)`; `func config.ResolveMQTT(ctx context.Context, c *Config, supervisorToken, supervisorURL string, hc *http.Client, lookup LookupHost) error` (`lookup` nil = `net.DefaultResolver.LookupHost`; an unresolvable Supervisor host falls back to `127.0.0.1`)

Notes: `/data/options.json` is parsed with `encoding/json` first (yaml.v3 rejects valid JSON such as `\/` and surrogate-pair escapes that Python's `json.dumps` emits), then re-encoded and decoded with the YAML decoder so unknown keys still fail. JSON map keys are strings, so `KeyNames` parses numeric keys from strings in every form.

- [ ] **Step 1: Add the YAML dependency**

Run: `go get gopkg.in/yaml.v3@v3.0.1`

- [ ] **Step 2: Write failing tests**

`internal/config/config_test.go`:

```go
package config_test

import (
	"context"
	"errors"
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

func TestKeyNamesStringForm(t *testing.T) {
	opts := write(t, "options.json", `{"lock_settings":[{"lock":"Front door","key_names":"1=Alice, 3=Bob"}]}`)
	cfg, err := config.Load(config.Sources{OptionsFile: opts, Environ: []string{
		`LOQED_LOCK_SETTINGS={"Back door":{"key_names":"2=Carol"}}`,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if f := cfg.LockSettings["Front door"].KeyNames; f[1] != "Alice" || f[3] != "Bob" {
		t.Fatalf("front: %+v", f)
	}
	if b := cfg.LockSettings["Back door"].KeyNames; b[2] != "Carol" {
		t.Fatalf("back: %+v", b)
	}
	if _, err := config.Load(config.Sources{ConfigFile: write(t, "c.yaml", "lock_settings: {x: {key_names: \"Alice\"}}\n")}); err == nil {
		t.Fatal("expected error for key_names without ids")
	}
}

// Python's json.dumps (used by the Supervisor) escapes "/" and non-ASCII;
// yaml.v3 rejects some of those escapes, so options.json is parsed as JSON.
func TestOptionsJSONEscapes(t *testing.T) {
	opts := write(t, "options.json", "{\"cloud_password\":\"p\\u00e4ss\\ud83d\\ude00\",\"webhook\":{\"public_url\":\"https:\\/\\/x.example\"},\"cloud_budget\":5,\"cache_max_age\":0,\"mqtt\":{\"url\":null}}")
	cfg, err := config.Load(config.Sources{OptionsFile: opts})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.CloudPassword != "päss😀" || cfg.Webhook.PublicURL != "https://x.example" || cfg.CloudBudget != 5 {
		t.Fatalf("got %+v", cfg)
	}
	bad := write(t, "options.json", `{"cloud_tokn":"x"}`)
	if _, err := config.Load(config.Sources{OptionsFile: bad}); err == nil {
		t.Fatal("unknown keys in options.json must fail")
	}
	empty := write(t, "options.json", "")
	if _, err := config.Load(config.Sources{OptionsFile: empty}); err != nil {
		t.Fatalf("empty options.json: %v", err)
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
	emailOnly := config.Defaults()
	emailOnly.CloudEmail = "me@example.com"
	if err := emailOnly.Validate(); err != nil {
		t.Fatalf("e-mail without password is allowed (cached minted token): %v", err)
	}
	bad := config.Defaults()
	bad.CloudPassword = "pw"
	bad.CloudBudget = 20
	bad.LivenessInterval = config.Duration(time.Second)
	bad.Webhook.PublicURL = "loqed.example.com"
	bad.Webhook.PrivateURL = "http://10.0.0.5:8099/prefix"
	bad.LogFormat = "xml"
	bad.Webhook.CloudSecret = "short"
	bad.MQTT.BaseTopic = "loqed/#"
	bad.LogLevel = "chatty"
	bad.LockSettings = config.LockSettingsMap{"x": {BridgeIP: "LOQED-aa.local", BridgeKey: "%%%"}}
	err := bad.Validate()
	if err == nil {
		t.Fatal("expected errors")
	}
	for _, want := range []string{"cloud_password", "cloud_budget", "liveness_interval", "public_url", "private_url must not have a path", "cloud_secret", "base_topic", "log_level", "log_format", "bridge_ip", "bridge_key"} {
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
	c.CloudEmail = "a"
	if !c.HasCloudCredentials() || c.CanMint() {
		t.Fatal("e-mail alone counts (cached minted token) but cannot mint")
	}
	c.CloudPassword = "b"
	if !c.CanMint() {
		t.Fatal("email+password can mint")
	}
}

func resolves(context.Context, string) ([]string, error) { return []string{"172.30.32.1"}, nil }

func TestResolveMQTTFallsBackToLoopbackWhenHostDoesNotResolve(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"result":"ok","data":{"host":"core-mosquitto","port":1883,"ssl":false,"username":"addons","password":"pw"}}`))
	}))
	defer srv.Close()
	c := config.Defaults()
	fail := func(context.Context, string) ([]string, error) { return nil, errors.New("no such host") }
	if err := config.ResolveMQTT(context.Background(), &c, "sup", srv.URL, srv.Client(), fail); err != nil {
		t.Fatal(err)
	}
	if c.MQTT.URL != "tcp://127.0.0.1:1883" {
		t.Fatalf("got %q", c.MQTT.URL)
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
	if err := config.ResolveMQTT(ctx, &c, "sup", srv.URL, srv.Client(), resolves); err != nil {
		t.Fatal(err)
	}
	if c.MQTT.URL != "tcp://core-mosquitto:1883" || c.MQTT.Username != "addons" || c.MQTT.Password != "pw" {
		t.Fatalf("got %+v", c.MQTT)
	}

	c = config.Defaults()
	c.MQTT.URL = "tcp://mine:1883"
	if err := config.ResolveMQTT(ctx, &c, "sup", srv.URL, srv.Client(), resolves); err != nil || c.MQTT.URL != "tcp://mine:1883" {
		t.Fatalf("explicit URL must win: %v %+v", err, c.MQTT)
	}

	c = config.Defaults()
	if err := config.ResolveMQTT(ctx, &c, "", srv.URL, srv.Client(), resolves); err != nil || c.MQTT.URL != config.DefaultMQTTURL {
		t.Fatalf("no supervisor → default: %v %+v", err, c.MQTT)
	}

	c = config.Defaults()
	if err := config.ResolveMQTT(ctx, &c, "wrong", srv.URL, srv.Client(), resolves); err == nil {
		t.Fatal("expected error")
	}
}
```

- [ ] **Step 3: Run to verify failure**

Run: `go test ./internal/config/`
Expected: FAIL — `no non-test Go files in …/internal/config`.

- [ ] **Step 4: Implement `config.go`**

`internal/config/config.go`:

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
				ID   int    `yaml:"id"`
				Name string `yaml:"name"`
			}
			if err := item.Decode(&e); err != nil {
				return err
			}
			(*k)[e.ID] = e.Name
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
```

- [ ] **Step 5: Implement `load.go`**

`internal/config/load.go`:

```go
package config

import (
	"bytes"
	"encoding/json"
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
		if err := decodeFile(src.OptionsFile, &cfg, true, true); err != nil {
			return Config{}, err
		}
	}
	if src.ConfigFile != "" {
		if err := decodeFile(src.ConfigFile, &cfg, false, false); err != nil {
			return Config{}, err
		}
	}
	if err := applyEnv(&cfg, src.Environ); err != nil {
		return Config{}, err
	}
	cfg.fillDefaults()
	return cfg, nil
}

// decodeFile overlays a YAML file onto cfg. With isJSON the file is parsed
// as JSON first (yaml.v3 rejects valid JSON such as "\/" or surrogate-pair
// escapes, which Python's json.dumps emits for the add-on options), then
// applied through the same YAML decoder so unknown keys still fail.
func decodeFile(path string, cfg *Config, optional, isJSON bool) error {
	b, err := os.ReadFile(path) //nolint:gosec // G304: path is the operator's own config file
	if optional && errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	if isJSON && len(bytes.TrimSpace(b)) > 0 {
		var v any
		if err := json.Unmarshal(b, &v); err != nil {
			return fmt.Errorf("config: %s: %w", path, err)
		}
		if b, err = yaml.Marshal(v); err != nil {
			return fmt.Errorf("config: %s: %w", path, err)
		}
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

`internal/config/supervisor.go`:

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

// LookupHost resolves a host name (net.DefaultResolver.LookupHost in production).
type LookupHost func(ctx context.Context, host string) ([]string, error)

// ResolveMQTT fills mqtt.url (and credentials if unset). An explicit URL
// wins; under the HA Supervisor the MQTT service is queried; otherwise
// DefaultMQTTURL is used. A Supervisor-provided host that does not resolve
// (host-network add-ons may not see the Supervisor DNS) falls back to
// 127.0.0.1, where the Mosquitto add-on also listens. lookup may be nil.
func ResolveMQTT(ctx context.Context, c *Config, supervisorToken, supervisorURL string, hc *http.Client, lookup LookupHost) error {
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
	defer func() { _ = resp.Body.Close() }()
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
	if lookup == nil {
		lookup = net.DefaultResolver.LookupHost
	}
	host := out.Data.Host
	if _, err := lookup(ctx, host); err != nil {
		host = "127.0.0.1"
	}
	c.MQTT.URL = scheme + "://" + net.JoinHostPort(host, strconv.Itoa(out.Data.Port))
	if c.MQTT.Username == "" {
		c.MQTT.Username, c.MQTT.Password = out.Data.Username, out.Data.Password
	}
	return nil
}
```

- [ ] **Step 7: Run tests**

Run: `gofmt -l internal/config && go test ./internal/config/ -v -race`
Expected: no gofmt output; PASS (13 tests). If `TestLockSettingsForms` fails on the inline struct, confirm the embedded field is written exactly `LockSetting \`yaml:",inline"\``.

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
  - `type store.LockRecord struct{ ID, Name, ModelName, BridgeIP, BridgeHostname, BridgeMacWifi string; LocalID *int; KeySecret, BridgeKey, BackendKey string }` + `HasLocalCredentials() bool` (IP, both keys, `LocalID` in 0..255)
  - `func store.FromCloud(cloud.Lock) LockRecord`; `func store.Merge(old, fresh LockRecord) LockRecord` (keeps old local-credential fields that fresh lacks); `func store.SameLocal(a, b LockRecord) bool`
  - `type store.MintedToken struct{ ID, Value, EmailSHA256 string; MintedAt time.Time }`
  - `type store.BudgetState struct{ Calls []time.Time; BlockedUntil time.Time }`
  - `type store.Cache struct{ Version int; InstallID, TokenSHA256 string; Minted *MintedToken; LastMintAt time.Time; CloudSecret string; FetchedAt time.Time; PublishedIDs []string; Budget BudgetState; Locks []LockRecord }` + `Find(key string) (LockRecord, bool)` (by id, then name)
  - `type store.Status int`: `StatusLoaded`, `StatusMissing`, `StatusCorrupt`, `StatusUnknownVersion`
  - `var store.ErrWrite` (wrapped by `Update` when the file cannot be written; the in-memory change is kept)
  - `func store.Open(path string) (*Store, Status, error)`; `(*Store).Snapshot() Cache` (deep copy); `(*Store).Update(func(*Cache)) error`; `(*Store).InstallID() (string, error)` (8 hex chars, created once); `(*Store).Path() string`
  - `func store.TokenHash(token string) string` (hex SHA-256); `func store.EmailHash(email string) string` (of the trimmed, lower-cased e-mail)

- [ ] **Step 1: Write failing tests**

`internal/store/store_test.go`:

```go
package store_test

import (
	"errors"
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
	if _, status, _ := store.Open(path); status != store.StatusUnknownVersion {
		t.Fatalf("unknown version: got %v", status)
	}
}

func TestBudgetPublishedIDsAndMintPersist(t *testing.T) {
	path := filepath.Join(t.TempDir(), "locks.json")
	st, _, _ := store.Open(path)
	t0 := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	err := st.Update(func(c *store.Cache) {
		c.Budget = store.BudgetState{Calls: []time.Time{t0, t0.Add(time.Minute)}, BlockedUntil: t0.Add(12 * time.Hour)}
		c.PublishedIDs = []string{"lock1", "lock2"}
		c.Minted = &store.MintedToken{ID: "t1", Value: "v", EmailSHA256: store.EmailHash(" Me@Example.com "), MintedAt: t0}
		c.LastMintAt = t0
	})
	if err != nil {
		t.Fatal(err)
	}
	again, _, _ := store.Open(path)
	c := again.Snapshot()
	if len(c.Budget.Calls) != 2 || !c.Budget.BlockedUntil.Equal(t0.Add(12*time.Hour)) || len(c.PublishedIDs) != 2 ||
		c.Minted.EmailSHA256 != store.EmailHash("me@example.com") || !c.LastMintAt.Equal(t0) {
		t.Fatalf("got %+v", c)
	}
	snap := again.Snapshot()
	snap.Budget.Calls[0] = time.Time{}
	snap.PublishedIDs[0] = "x"
	if c2 := again.Snapshot(); c2.Budget.Calls[0].IsZero() || c2.PublishedIDs[0] != "lock1" {
		t.Fatal("snapshot shares slices with the store")
	}
}

func TestInstallIDIsStable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "locks.json")
	st, _, _ := store.Open(path)
	id, err := st.InstallID()
	if err != nil || len(id) != 8 {
		t.Fatalf("id %q err %v", id, err)
	}
	again, _, _ := store.Open(path)
	if id2, _ := again.InstallID(); id2 != id {
		t.Fatalf("install id changed: %q → %q", id, id2)
	}
}

func TestUpdateWriteFailureKeepsMemoryAndWrapsErrWrite(t *testing.T) {
	dir := t.TempDir()
	st, _, _ := store.Open(filepath.Join(dir, "locks.json"))
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	err := st.Update(func(c *store.Cache) { c.CloudSecret = "s" })
	if !errors.Is(err, store.ErrWrite) {
		t.Fatalf("got %v", err)
	}
	if st.Snapshot().CloudSecret != "s" {
		t.Fatal("in-memory update lost")
	}
}

func TestMergeKeepsLocalCredentials(t *testing.T) {
	id := 3
	old := store.LockRecord{ID: "a", Name: "Old", BridgeIP: "192.0.2.1", KeySecret: "k", BridgeKey: "b", LocalID: &id}
	merged := store.Merge(old, store.LockRecord{ID: "a", Name: "New"})
	if merged.Name != "New" || !store.SameLocal(merged, old) {
		t.Fatalf("got %+v", merged)
	}
	changed := store.Merge(old, store.LockRecord{ID: "a", BridgeIP: "192.0.2.9"})
	if changed.BridgeIP != "192.0.2.9" || store.SameLocal(changed, old) {
		t.Fatalf("got %+v", changed)
	}
	bad := 300
	if (store.LockRecord{BridgeIP: "x", KeySecret: "k", BridgeKey: "b", LocalID: &bad}).HasLocalCredentials() {
		t.Fatal("local_id 300 is not usable")
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
Expected: FAIL — `no non-test Go files in …/internal/store`.

- [ ] **Step 3: Implement**

`internal/store/store.go`:

```go
// Package store persists cloud lock credentials so restarts do not need
// the (rate-limited) cloud.
package store

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
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
	return r.BridgeIP != "" && r.BridgeKey != "" && r.KeySecret != "" &&
		r.LocalID != nil && *r.LocalID >= 0 && *r.LocalID <= 255
}

// Merge returns fresh, keeping old's local-credential fields where fresh
// lacks them (the cloud's local fields are undocumented and may vanish).
func Merge(old, fresh LockRecord) LockRecord {
	if fresh.BridgeIP == "" {
		fresh.BridgeIP = old.BridgeIP
	}
	if fresh.KeySecret == "" {
		fresh.KeySecret = old.KeySecret
	}
	if fresh.BridgeKey == "" {
		fresh.BridgeKey = old.BridgeKey
	}
	if fresh.LocalID == nil && old.LocalID != nil {
		v := *old.LocalID
		fresh.LocalID = &v
	}
	return fresh
}

// SameLocal reports whether two records address the bridge identically.
func SameLocal(a, b LockRecord) bool {
	return a.BridgeIP == b.BridgeIP && a.KeySecret == b.KeySecret && a.BridgeKey == b.BridgeKey &&
		(a.LocalID == nil) == (b.LocalID == nil) && (a.LocalID == nil || *a.LocalID == *b.LocalID)
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

// MintedToken is a token this gateway created via the portal.
type MintedToken struct {
	ID          string    `json:"id"`
	Value       string    `json:"value"`
	EmailSHA256 string    `json:"email_sha256,omitempty"` // account it belongs to (EmailHash)
	MintedAt    time.Time `json:"minted_at"`
}

// BudgetState persists the cloud request window across restarts.
type BudgetState struct {
	Calls        []time.Time `json:"calls,omitempty"`
	BlockedUntil time.Time   `json:"blocked_until,omitzero"`
}

type Cache struct {
	Version      int          `json:"version"`
	InstallID    string       `json:"install_id,omitempty"`
	TokenSHA256  string       `json:"token_sha256,omitempty"`
	Minted       *MintedToken `json:"minted_token,omitempty"`
	LastMintAt   time.Time    `json:"last_mint_at,omitzero"` // includes failed attempts
	CloudSecret  string       `json:"cloud_secret,omitempty"`
	FetchedAt    time.Time    `json:"fetched_at"`
	PublishedIDs []string     `json:"published_ids,omitempty"`
	Budget       BudgetState  `json:"budget"`
	Locks        []LockRecord `json:"locks"`
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
	out.PublishedIDs = slices.Clone(c.PublishedIDs)
	out.Budget.Calls = slices.Clone(c.Budget.Calls)
	return out
}

type Status int

const (
	StatusLoaded Status = iota
	StatusMissing
	StatusCorrupt
	StatusUnknownVersion // written by a newer or older release; treated as missing
)

// ErrWrite reports that the cache could not be persisted. In-memory state
// is still updated, so the gateway keeps running.
var ErrWrite = errors.New("store: cannot write the credential cache")

type Store struct {
	path  string
	mu    sync.Mutex
	cache Cache
}

// Open loads the cache. Missing or corrupt files yield an empty cache and
// the matching Status; only unexpected I/O errors are returned.
func Open(path string) (*Store, Status, error) {
	s := &Store{path: path, cache: Cache{Version: Version}}
	b, err := os.ReadFile(path) //nolint:gosec // G304: path comes from configuration
	if errors.Is(err, fs.ErrNotExist) {
		return s, StatusMissing, nil
	}
	if err != nil {
		return nil, 0, fmt.Errorf("store: %w", err)
	}
	var c Cache
	if json.Unmarshal(b, &c) != nil {
		return s, StatusCorrupt, nil
	}
	if c.Version != Version {
		return s, StatusUnknownVersion, nil
	}
	s.cache = c
	return s, StatusLoaded, nil
}

// Path is the cache file location (for messages).
func (s *Store) Path() string { return s.path }

func (s *Store) Snapshot() Cache {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cache.clone()
}

// Update applies fn and persists the result. The in-memory cache is
// updated even when writing fails (the error wraps ErrWrite).
func (s *Store) Update(fn func(*Cache)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	fn(&s.cache)
	s.cache.Version = Version
	if err := write(s.path, s.cache); err != nil {
		return fmt.Errorf("%w %s: %w", ErrWrite, s.path, err)
	}
	return nil
}

// InstallID returns this installation's stable random id, creating and
// persisting it on first use. It names minted tokens.
func (s *Store) InstallID() (string, error) {
	s.mu.Lock()
	id := s.cache.InstallID
	s.mu.Unlock()
	if id != "" {
		return id, nil
	}
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	id = hex.EncodeToString(b)
	err := s.Update(func(c *Cache) {
		if c.InstallID == "" {
			c.InstallID = id
		}
		id = c.InstallID
	})
	return id, err
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
	defer func() { _ = os.Remove(tmp) }() // no-op after a successful rename
	if err := f.Chmod(0o600); err != nil {
		_ = f.Close()
		return fmt.Errorf("store: %w", err)
	}
	if _, err := f.Write(b); err != nil {
		_ = f.Close()
		return fmt.Errorf("store: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
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

// EmailHash identifies the portal account a minted token belongs to.
func EmailHash(email string) string {
	return TokenHash(strings.ToLower(strings.TrimSpace(email)))
}
```

- [ ] **Step 4: Run tests**

Run: `gofmt -l internal/store && go test ./internal/store/ -v -race`
Expected: PASS (8 tests). `TestUpdateWriteFailureKeepsMemoryAndWrapsErrWrite` skips when run as root.

- [ ] **Step 5: Commit**

```bash
git add internal/store
git commit -m "store: add the atomic credential cache

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: Cloud token resolution and minting

**Files:**
- Create: `internal/auth/auth.go`, `internal/auth/auth_test.go`

**Interfaces:**
- Consumes: `store.Store`, `store.MintedToken`, `store.EmailHash`, `portal.Client`, `portal.Token`, `portal.TokenInfo`, `loqed.ErrUnauthorized`.
- Produces:
  - `var auth.ErrNoToken`
  - `type auth.Minter interface{ Mint(ctx context.Context) (store.MintedToken, error) }`
  - `type auth.PortalSession interface{ ListTokens(ctx) ([]portal.TokenInfo, error); CreateToken(ctx, name string) (portal.Token, error); RevokeToken(ctx, id string) error; Logout(ctx) error }`
  - `type auth.PortalMinter struct{ Login func(ctx context.Context, email, password string) (PortalSession, error); Email, Password, TokenName string; Log *slog.Logger }` — `Mint` creates the new token **first**, then revokes other tokens with the same name, then logs out
  - `func auth.NewPortalMinter(c *portal.Client, email, password, tokenName string, log *slog.Logger) *PortalMinter`
  - `func auth.TokenName(installID string) string` → `loqed-mqtt <install-id>`
  - `const auth.RemintInterval = time.Hour` (enforced through `store.Cache.LastMintAt`, so it holds across restarts)
  - `func auth.NewResolver(configured, email string, minter Minter, st *store.Store, now func() time.Time, log *slog.Logger) *Resolver` (`minter` nil when the password is not configured)
  - `func (*Resolver) Token(ctx) (string, error)`; `func (*Resolver) Invalidate(ctx, rejected string) (string, error)` — a cached minted token is used only if its `EmailSHA256` matches the configured e-mail

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
	r := auth.NewResolver("configured", "", m, newStore(t), time.Now, discard)
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
	_ = st.Update(func(c *store.Cache) {
		c.Minted = &store.MintedToken{ID: "x", Value: "cached", EmailSHA256: store.EmailHash("me@example.com")}
	})
	m := &fakeMinter{}
	tok, err := auth.NewResolver("", "me@example.com", m, st, time.Now, discard).Token(context.Background())
	if err != nil || tok != "cached" || m.calls != 0 {
		t.Fatalf("%q %v calls=%d", tok, err, m.calls)
	}
}

func TestMintsAndPersists(t *testing.T) {
	st := newStore(t)
	m := &fakeMinter{}
	tok, err := auth.NewResolver("", "me@example.com", m, st, time.Now, discard).Token(context.Background())
	if err != nil || tok != "minted-1" {
		t.Fatalf("%q %v", tok, err)
	}
	if got := st.Snapshot().Minted; got == nil || got.Value != "minted-1" {
		t.Fatalf("not persisted: %+v", got)
	}
}

func TestNoCredentials(t *testing.T) {
	if _, err := auth.NewResolver("", "", nil, newStore(t), time.Now, discard).Token(context.Background()); !errors.Is(err, auth.ErrNoToken) {
		t.Fatalf("got %v", err)
	}
}

func TestInvalidateRemintsAtMostHourly(t *testing.T) {
	now := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	m := &fakeMinter{}
	r := auth.NewResolver("", "me@example.com", m, newStore(t), func() time.Time { return now }, discard)
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
	tokens    []portal.TokenInfo
	revoked   []string
	created   []string
	order     []string
	createErr error
	logout    bool
}

func (s *fakeSession) ListTokens(context.Context) ([]portal.TokenInfo, error) {
	s.order = append(s.order, "list")
	return s.tokens, nil
}
func (s *fakeSession) CreateToken(_ context.Context, name string) (portal.Token, error) {
	s.order = append(s.order, "create")
	if s.createErr != nil {
		return portal.Token{}, s.createErr
	}
	s.created = append(s.created, name)
	tok := portal.Token{ID: "new", Name: name, Value: "pat"}
	s.tokens = append(s.tokens, portal.TokenInfo{ID: tok.ID, Name: name})
	return tok, nil
}
func (s *fakeSession) RevokeToken(_ context.Context, id string) error {
	s.order = append(s.order, "revoke")
	s.revoked = append(s.revoked, id)
	return nil
}
func (s *fakeSession) Logout(context.Context) error { s.logout = true; return nil }

func TestPortalMinterCreatesBeforeRevoking(t *testing.T) {
	name := auth.TokenName("1a2b3c4d")
	sess := &fakeSession{tokens: []portal.TokenInfo{{ID: "old", Name: name}, {ID: "keep", Name: "HA"}}}
	m := &auth.PortalMinter{
		Login: func(_ context.Context, email, password string) (auth.PortalSession, error) {
			if email != "me" || password != "pw" {
				t.Fatalf("credentials %q %q", email, password)
			}
			return sess, nil
		},
		Email: "me", Password: "pw", TokenName: name,
	}
	tok, err := m.Mint(context.Background())
	if err != nil || tok.ID != "new" || tok.Value != "pat" {
		t.Fatalf("%+v %v", tok, err)
	}
	if len(sess.revoked) != 1 || sess.revoked[0] != "old" || sess.created[0] != "loqed-mqtt 1a2b3c4d" || !sess.logout {
		t.Fatalf("session %+v", sess)
	}
	if sess.order[0] != "create" {
		t.Fatalf("must create before revoking: %v", sess.order)
	}
}

func TestPortalMinterFailedCreateRevokesNothing(t *testing.T) {
	name := auth.TokenName("1a2b3c4d")
	sess := &fakeSession{tokens: []portal.TokenInfo{{ID: "old", Name: name}}, createErr: loqed.ErrInvalidPayload}
	m := &auth.PortalMinter{
		Login:     func(context.Context, string, string) (auth.PortalSession, error) { return sess, nil },
		TokenName: name,
	}
	if _, err := m.Mint(context.Background()); !errors.Is(err, loqed.ErrInvalidPayload) {
		t.Fatalf("got %v", err)
	}
	if len(sess.revoked) != 0 || !sess.logout {
		t.Fatalf("session %+v", sess)
	}
}

func TestMintedTokenForAnotherAccountIsNotUsed(t *testing.T) {
	st := newStore(t)
	_ = st.Update(func(c *store.Cache) {
		c.Minted = &store.MintedToken{ID: "x", Value: "other-account", EmailSHA256: store.EmailHash("old@example.com")}
	})
	m := &fakeMinter{}
	tok, err := auth.NewResolver("", "me@example.com", m, st, time.Now, discard).Token(context.Background())
	if err != nil || tok != "minted-1" || m.calls != 1 {
		t.Fatalf("%q %v calls=%d", tok, err, m.calls)
	}
	if got := st.Snapshot().Minted; got.EmailSHA256 != store.EmailHash("ME@example.com") {
		t.Fatalf("minted token not tagged with the account: %+v", got)
	}
}

func TestEmailWithoutPasswordUsesCachedTokenButCannotMint(t *testing.T) {
	st := newStore(t)
	_ = st.Update(func(c *store.Cache) {
		c.Minted = &store.MintedToken{Value: "cached", EmailSHA256: store.EmailHash("me@example.com")}
	})
	r := auth.NewResolver("", "me@example.com", nil, st, time.Now, discard)
	if tok, err := r.Token(context.Background()); err != nil || tok != "cached" {
		t.Fatalf("%q %v", tok, err)
	}
	if _, err := r.Invalidate(context.Background(), "cached"); !errors.Is(err, auth.ErrNoToken) {
		t.Fatalf("got %v", err)
	}
}

func TestMintLimitSurvivesRestart(t *testing.T) {
	now := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	st := newStore(t)
	m := &fakeMinter{}
	first := auth.NewResolver("", "me@example.com", m, st, func() time.Time { return now }, discard)
	tok, _ := first.Token(context.Background())
	// A restarted process with the same cache must not mint again within the hour.
	second := auth.NewResolver("", "me@example.com", m, st, func() time.Time { return now.Add(time.Minute) }, discard)
	if _, err := second.Invalidate(context.Background(), tok); !errors.Is(err, loqed.ErrUnauthorized) || m.calls != 1 {
		t.Fatalf("err %v calls=%d", err, m.calls)
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/auth/`
Expected: FAIL — `no non-test Go files in …/internal/auth`.

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

// RemintInterval limits how often a token is minted (persisted, so it also
// holds across restarts).
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
	Log       *slog.Logger
}

func NewPortalMinter(c *portal.Client, email, password, tokenName string, log *slog.Logger) *PortalMinter {
	return &PortalMinter{
		Login: func(ctx context.Context, email, password string) (PortalSession, error) {
			s, err := c.Login(ctx, email, password)
			if err != nil {
				return nil, err
			}
			return s, nil
		},
		Email: email, Password: password, TokenName: tokenName, Log: log,
	}
}

// TokenName names minted tokens after the installation id, which is
// stable across container re-creation (a Docker hostname is not).
func TokenName(installID string) string { return "loqed-mqtt " + installID }

// Mint logs in, creates a new token, then revokes older tokens with the
// same name and logs out. Creating first means a failed create never
// leaves the user without a working token.
func (m *PortalMinter) Mint(ctx context.Context) (store.MintedToken, error) {
	s, err := m.Login(ctx, m.Email, m.Password)
	if err != nil {
		return store.MintedToken{}, err
	}
	defer func() { _ = s.Logout(context.WithoutCancel(ctx)) }()
	tok, err := s.CreateToken(ctx, m.TokenName)
	if err != nil {
		return store.MintedToken{}, err
	}
	existing, err := s.ListTokens(ctx)
	if err != nil {
		m.logWarn("could not list old LOQED tokens to revoke", err)
		return store.MintedToken{ID: tok.ID, Value: tok.Value}, nil
	}
	for _, t := range existing {
		if t.Name == m.TokenName && t.ID != tok.ID && tok.ID != "" {
			if err := s.RevokeToken(ctx, t.ID); err != nil {
				m.logWarn("could not revoke an old LOQED token", err)
			}
		}
	}
	return store.MintedToken{ID: tok.ID, Value: tok.Value}, nil
}

func (m *PortalMinter) logWarn(msg string, err error) {
	if m.Log != nil {
		m.Log.Warn(msg, "err", err)
	}
}

type Resolver struct {
	configured string
	emailHash  string // "" when cloud_email is not configured
	minter     Minter // nil when minting is impossible
	store      *store.Store
	now        func() time.Time
	log        *slog.Logger

	mu sync.Mutex
}

// NewResolver: configured is cloud_token; email is cloud_email; minter is
// nil unless both cloud_email and cloud_password are set.
func NewResolver(configured, email string, minter Minter, st *store.Store, now func() time.Time, log *slog.Logger) *Resolver {
	r := &Resolver{configured: configured, minter: minter, store: st, now: now, log: log}
	if email != "" {
		r.emailHash = store.EmailHash(email)
	}
	return r
}

// Token returns cloud_token, else the cached minted token (if it belongs
// to the configured account), else mints one.
func (r *Resolver) Token(ctx context.Context) (string, error) {
	if r.configured != "" {
		return r.configured, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if v := r.cachedLocked(); v != "" {
		return v, nil
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
	if v := r.cachedLocked(); v != "" && v != rejected {
		return v, nil
	}
	return r.mintLocked(ctx)
}

func (r *Resolver) cachedLocked() string {
	m := r.store.Snapshot().Minted
	if m == nil || m.Value == "" {
		return ""
	}
	if r.emailHash != "" && m.EmailSHA256 != r.emailHash {
		return "" // minted for a different account
	}
	return m.Value
}

func (r *Resolver) mintLocked(ctx context.Context) (string, error) {
	if r.minter == nil {
		if r.emailHash != "" {
			return "", fmt.Errorf("%w (cloud_password is needed to create a token for cloud_email)", ErrNoToken)
		}
		return "", ErrNoToken
	}
	now := r.now()
	if last := r.store.Snapshot().LastMintAt; !last.IsZero() && now.Sub(last) < RemintInterval && !now.Before(last) {
		return "", fmt.Errorf("%w: token rejected; next attempt to create one after %s",
			loqed.ErrUnauthorized, last.Add(RemintInterval).Format(time.RFC3339))
	}
	// Persist the attempt before minting so restarts cannot bypass the limit.
	if err := r.store.Update(func(c *store.Cache) { c.LastMintAt = now }); err != nil {
		r.log.Warn("could not save the token mint time", "err", err)
	}
	tok, err := r.minter.Mint(ctx)
	if err != nil {
		return "", fmt.Errorf("auth: creating a token with cloud_email/cloud_password failed (set cloud_token to bypass): %w", err)
	}
	tok.EmailSHA256, tok.MintedAt = r.emailHash, now
	if err := r.store.Update(func(c *store.Cache) { c.Minted = &tok }); err != nil {
		r.log.Warn("could not save the new LOQED token", "err", err)
	}
	r.log.Info("created a LOQED personal access token", "token_id", tok.ID)
	return tok.Value, nil
}
```

- [ ] **Step 4: Run tests**

Run: `gofmt -l internal/auth && go test ./internal/auth/ -v -race`
Expected: PASS (10 tests).

- [ ] **Step 5: Commit**

```bash
git add internal/auth
git commit -m "auth: resolve LOQED tokens and mint them via the portal

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: State model and LOQED → HA mapping

**Files:**
- Create: `internal/model/model.go`, `internal/model/mapping.go`, `internal/model/model_test.go`

**Interfaces:**
- Consumes: `loqed.BoltState`, `loqed.ReachedState`.
- Produces:
  - `type model.Mode string`: `ModeLocal="local"`, `ModeCloud="cloud"`, `ModeOffline="offline"`
  - `type model.LockState string`: `Locked="LOCKED"`, `Unlocked="UNLOCKED"`, `Open="OPEN"`, `Locking="LOCKING"`, `Unlocking="UNLOCKING"`, `Opening="OPENING"`, `Jammed="JAMMED"`
  - `type model.EventType string`: `EventLocked="locked"`, `EventUnlocked="unlocked"`, `EventOpened="opened"`, `EventLocking="locking"`, `EventUnlocking="unlocking"`, `EventOpening="opening"`, `EventJammed="jammed"`, `EventUnknown="unknown"`, `EventCommandFailed="command_failed"`; `var model.EventTypes []EventType` (that order)
  - `const model.SourceGateway = "gateway"`; failure classes `FailExpired="expired"`, `FailOffline="offline"`, `FailUnreachable="unreachable"`, `FailNoResponse="no_response"`, `FailUnauthorized="unauthorized"`, `FailRateLimited="rate_limited"`, `FailOther="failed"`
  - `type model.State struct{ Lock *LockState; BoltState loqed.BoltState; BatteryPercentage *int; BatteryVoltage *float64; WifiStrength, BLEStrength *int; LockOnline bool; Mode Mode; LastEvent string; LastKeyID *int; LastKeyName *string; LastEventAt *time.Time; StateStale bool }` (JSON names per spec §6.1 plus `last_key_name`)
  - `type model.Event struct{ EventType EventType; Reason, Source string; KeyLocalID *int; KeyName *string; Error string }` (`error` omitted when empty)
  - `type model.Command string`: `CommandLock="LOCK"`, `CommandUnlock="UNLOCK"`, `CommandOpen="OPEN"`; `func ParseCommand(string) (Command, bool)`; `func (Command) Target() loqed.BoltState`; `func (Command) Moving() LockState`
  - `type model.Transition struct{ SetLock bool; Lock *LockState; SetBolt bool; Bolt loqed.BoltState; Event EventType }`; `func (*State) Apply(Transition)`
  - `func LockStateFor(loqed.BoltState) *LockState` (unknown → nil)
  - `func FromStateReached(eventType string) Transition` — bolt from `loqed.ReachedState(eventType)`; `MOTOR_STALL` → `JAMMED` without touching the bolt
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
		lock      string
		bolt      loqed.BoltState
		event     model.EventType
	}{
		{"STATE_CHANGED_NIGHT_LOCK", "LOCKED", loqed.BoltNightLock, model.EventLocked},
		{"STATE_CHANGED_LATCH", "UNLOCKED", loqed.BoltDayLock, model.EventUnlocked},
		{"STATE_CHANGED_OPEN_REMOTE", "OPEN", loqed.BoltOpen, model.EventOpened},
		{"STATE_CHANGED_NIGHT_LOCK_REMOTE", "LOCKED", loqed.BoltNightLock, model.EventLocked},
		{"STATE_CHANGED_UNKNOWN", "<nil>", loqed.BoltUnknown, model.EventUnknown},
		{"SOMETHING_NEW", "<nil>", loqed.BoltUnknown, model.EventUnknown},
	}
	for _, c := range cases {
		tr := model.FromStateReached(c.eventType)
		if !tr.SetLock || lockStr(tr.Lock) != c.lock || !tr.SetBolt || tr.Bolt != c.bolt || tr.Event != c.event {
			t.Errorf("%s: %+v (lock %s)", c.eventType, tr, lockStr(tr.Lock))
		}
	}
	stall := model.FromStateReached("MOTOR_STALL")
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
	s.Apply(model.FromStateReached("STATE_CHANGED_UNKNOWN"))
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
Expected: FAIL — `no non-test Go files in …/internal/model`.

- [ ] **Step 3: Implement `model.go`**

`internal/model/model.go`:

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
	// EventCommandFailed is emitted by the gateway when a LOCK/UNLOCK/OPEN
	// command could not be executed (or its outcome is uncertain).
	EventCommandFailed EventType = "command_failed"
)

var EventTypes = []EventType{EventLocked, EventUnlocked, EventOpened, EventLocking, EventUnlocking, EventOpening, EventJammed, EventUnknown, EventCommandFailed}

// SourceGateway marks events produced by the gateway itself.
const SourceGateway = "gateway"

// Command failure classes published in Event.Error.
const (
	FailExpired      = "expired"      // older than CommandMaxAge before it could be sent
	FailOffline      = "offline"      // no path to the lock
	FailUnreachable  = "unreachable"  // not delivered
	FailNoResponse   = "no_response"  // maybe delivered; outcome being verified
	FailUnauthorized = "unauthorized" // credentials rejected
	FailRateLimited  = "rate_limited" // cloud rate limit
	FailOther        = "failed"
)

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
	Error      string    `json:"error,omitempty"` // command_failed only
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

`internal/model/mapping.go`:

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
// other "state reached" events. The bolt state comes from the event type
// (loqed.ReachedState), never from requested_state.
func FromStateReached(eventType string) Transition {
	b, jammed := loqed.ReachedState(eventType)
	if jammed {
		return Transition{SetLock: true, Lock: Ptr(Jammed), Event: EventJammed}
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
		t.Event = EventUnknown
	}
	return t
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

Run: `gofmt -l internal/model && go test ./internal/model/ -v`
Expected: PASS (7 tests).

- [ ] **Step 6: Commit**

```bash
git add internal/model
git commit -m "model: add the state document and LOQED to HA mapping

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
	if hass.TopicID("Yq1g/K4+#x y") != "Yq1g_K4__x_y" {
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
Expected: FAIL — `no non-test Go files in …/internal/hass`.

- [ ] **Step 3: Implement `topics.go`**

`internal/hass/topics.go`:

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
func (t Topics) Discovery(id string) string {
	return t.DiscoveryPrefix + "/device/loqed_" + id + "/config"
}
func (t Topics) HAStatus() string { return t.DiscoveryPrefix + "/status" }

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

`internal/hass/discovery.go`:

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
Expected: PASS. Open `internal/hass/testdata/discovery_lock1.golden.json` and check by eye: 9 components, lock `"name": null`, `"availability_mode": "all"`, event `event_types` lists the 9 normalized types ending with `command_failed`.

- [ ] **Step 6: Commit**

```bash
git add internal/hass
git commit -m "hass: add topics and device discovery payload

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: MQTT client

**Files:**
- Create: `internal/testutil/mqtt.go`, `internal/hass/client.go`, `internal/hass/client_test.go`

**Interfaces:**
- Consumes: `Topics`, `TopicID`, `LockInfo`, `DiscoveryPayload`, `model.State`, `model.Event`, `model.ParseCommand`.
- Produces:
  - `type hass.ClientConfig struct{ URL, Username, Password, ClientID string; Topics Topics; HAEnabled bool; Version string; Now func() time.Time }` (`Now` stamps commands; nil = `time.Now`)
  - `type hass.Command struct{ LockID string; Command model.Command; At time.Time }` (LockID is the real cloud id)
  - `func hass.NewClient(cfg ClientConfig, log *slog.Logger) *Client`
  - `(*Client).Start()` (connects in the background, retrying forever), `Close()` (publishes retained `offline`), `Connected() bool`, `DisconnectedFor() time.Duration` (0 while connected; counted from start when never connected), `Commands() <-chan Command`
  - `(*Client).SetLocks(locks []LockInfo, removed []string)` — `removed` are real lock ids whose retained discovery, state and availability topics are cleared (state/availability even with HA disabled); may be called at runtime
  - `(*Client).PublishState(lockID string, s model.State) error`, `PublishEvent(lockID string, e model.Event) error`, `PublishAvailability(lockID string, online bool) error` (dedupes unchanged availability). While disconnected these return nil: state and availability are cached and republished on connect (under a lock, so an older document is never published after a newer one); events are dropped.
  - `func hass.RedactURL(raw string) string` (password → `xxxxx`)
  - Retained messages on the command topic are ignored and logged.
  - Test helpers: `testutil.StartBroker(t) string` (returns `tcp://127.0.0.1:port`), `testutil.Subscribe(t, url, filter string) *Subscriber`, `(*Subscriber).WaitFor(t, timeout, func(Message) bool) Message`, `(*Subscriber).Count(func(Message) bool) int`, `(*Subscriber).Publish(t, topic, payload string, retained bool)`, `type testutil.Message struct{ Topic string; Payload []byte; Retained bool }`, `testutil.Topic(topic) func(Message) bool`

- [ ] **Step 1: Add dependencies**

Run: `go get github.com/eclipse/paho.mqtt.golang@v1.5.1 github.com/mochi-mqtt/server/v2@v2.7.9`

- [ ] **Step 2: Write the test helpers**

`internal/testutil` is only imported from tests.

`internal/testutil/mqtt.go`:

```go
// Package testutil provides an in-process MQTT broker and subscriber for tests.
package testutil

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
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
	_ = ln.Close()
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

var subscribers atomic.Int64

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
	// Unique client ids: a broker disconnects an existing client when another
	// connects with the same id.
	id := fmt.Sprintf("sub-%s-%d", t.Name(), subscribers.Add(1))
	opts := mqtt.NewClientOptions().AddBroker(url).SetClientID(id).SetCleanSession(true)
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
	if c.DisconnectedFor() != 0 {
		t.Fatal("connected client reports downtime")
	}
	if err := c.PublishState("lock1", model.State{Lock: model.Ptr(model.Locked), Mode: model.ModeLocal}); err != nil {
		t.Fatal(err)
	}
	if err := c.PublishAvailability("lock1", true); err != nil {
		t.Fatal(err)
	}
	// A subscriber connecting later must see retained messages.
	sub := testutil.Subscribe(t, url, "#")
	sub.WaitFor(t, wait, func(m testutil.Message) bool {
		return m.Topic == "loqed/status" && string(m.Payload) == "online" && m.Retained
	})
	sub.WaitFor(t, wait, func(m testutil.Message) bool {
		return m.Topic == "homeassistant/device/loqed_lock1/config" && m.Retained && len(m.Payload) > 0
	})
	sub.WaitFor(t, wait, func(m testutil.Message) bool {
		return m.Topic == "loqed/lock1/availability" && string(m.Payload) == "online"
	})
	st := sub.WaitFor(t, wait, testutil.Topic("loqed/lock1/state"))
	var s model.State
	if err := json.Unmarshal(st.Payload, &s); err != nil || s.Lock == nil || *s.Lock != model.Locked {
		t.Fatalf("state %s %v", st.Payload, err)
	}
}

func TestRemovedLockTopicsAreCleared(t *testing.T) {
	url := testutil.StartBroker(t)
	sub := testutil.Subscribe(t, url, "#")
	startClient(t, url, true)
	for _, topic := range []string{"homeassistant/device/loqed_gone/config", "loqed/gone/state", "loqed/gone/availability"} {
		sub.WaitFor(t, wait, func(m testutil.Message) bool { return m.Topic == topic && len(m.Payload) == 0 })
	}
}

// A retained command (e.g. published by mistake with the retain flag) is
// redelivered on every reconnect; it must never actuate the lock.
func TestRetainedCommandIsIgnored(t *testing.T) {
	url := testutil.StartBroker(t)
	pub := testutil.Subscribe(t, url, "unused/#")
	pub.Publish(t, "loqed/lock1/command", "OPEN", true)
	c := startClient(t, url, true)
	time.Sleep(300 * time.Millisecond)
	pub.Publish(t, "loqed/lock1/command", "LOCK", false)
	select {
	case cmd := <-c.Commands():
		if cmd.Command != model.CommandLock {
			t.Fatalf("retained command delivered: %+v", cmd)
		}
	case <-time.After(wait):
		t.Fatal("live command not delivered")
	}
}

func TestRedactURL(t *testing.T) {
	if got := hass.RedactURL("tcp://user:s3cret@broker:1883"); got != "tcp://user:xxxxx@broker:1883" {
		t.Fatalf("got %q", got)
	}
}

func TestDisconnectedFor(t *testing.T) {
	now := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	c := hass.NewClient(hass.ClientConfig{URL: "tcp://127.0.0.1:1", ClientID: "x", Topics: topics,
		Now: func() time.Time { return now }}, slog.New(slog.DiscardHandler))
	now = now.Add(6 * time.Minute)
	if d := c.DisconnectedFor(); d != 6*time.Minute {
		t.Fatalf("never connected: %v", d)
	}
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

Run: `go test ./internal/hass/ -run 'Publish|Removed|Events|Commands|Retained|Birth|Disabled|Availability|Redact|Disconnected'`
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
	"net/url"
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

	mu        sync.Mutex
	locks     map[string]LockInfo // by topic id
	removed   []string            // real lock ids
	states    map[string][]byte   // by real lock id
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
		locks: map[string]LockInfo{}, states: map[string][]byte{}, avail: map[string]string{}}
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
// topics (discovery, state, availability) must be cleared; it is cleared
// again on every reconnect until the next SetLocks.
func (c *Client) SetLocks(locks []LockInfo, removed []string) {
	c.mu.Lock()
	c.locks = make(map[string]LockInfo, len(locks))
	for _, l := range locks {
		c.locks[TopicID(l.ID)] = l
	}
	c.removed = append([]string(nil), removed...)
	for _, id := range removed {
		delete(c.states, id)
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
	c.retainMu.Lock()
	defer c.retainMu.Unlock()
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
	for _, id := range removed {
		_ = c.publish(t.State(TopicID(id)), true, []byte{})
		_ = c.publish(t.Availability(TopicID(id)), true, []byte{})
		if c.cfg.HAEnabled {
			_ = c.publish(t.Discovery(TopicID(id)), true, []byte{})
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

Run: `gofmt -l internal && go test ./internal/hass/ -v -race`
Expected: PASS (12 tests). Broker tests take a few seconds.

- [ ] **Step 7: Commit**

```bash
git add go.mod go.sum internal/testutil internal/hass
git commit -m "hass: add the MQTT client with discovery, commands and republish

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: Cloud request budget and cloud hub

**Files:**
- Create: `internal/gateway/budget.go`, `internal/gateway/cloudhub.go`, `internal/gateway/budget_test.go`, `internal/gateway/cloudhub_test.go`

**Interfaces:**
- Consumes: `cloud.Lock`, `loqed.ErrUnauthorized`, `loqed.ErrRateLimited`, `loqed.BoltState`, `store.BudgetState`.
- Produces:
  - `type gateway.Priority int`: `PriorityConfirm` (may use the whole budget), `PriorityRefresh` (leaves 1), `PriorityBackground` (leaves 2, spaced `window/limit`)
  - `var gateway.ErrBudgetExhausted, ErrDeferred, ErrCloudBlocked`
  - `func gateway.NewBudget(limit int, window time.Duration, now func() time.Time, state store.BudgetState, save func(store.BudgetState)) *Budget` — restores `state` (future timestamps clamped to now; a block to at most `RateLimitBackoff` ahead) and calls `save` (may be nil) after every change; `(*Budget).Take(Priority) error`, `Record() int` (never refuses; returns calls in the window), `Block(time.Duration)`, `Remaining() int`, `Spacing() time.Duration`
  - `type gateway.CloudAPI interface{ ListLocks(ctx) ([]cloud.Lock, error); Command(ctx, lockID string, s loqed.BoltState) error }` (satisfied by `*cloud.Client`)
  - `type gateway.TokenSource interface{ Token(ctx) (string, error); Invalidate(ctx, rejected string) (string, error) }` (satisfied by `*auth.Resolver`)
  - `const gateway.RateLimitBackoff = 12 * time.Hour`
  - `type gateway.LockList struct{ Locks []cloud.Lock; FetchedAt time.Time }` (`FetchedAt` = when the request was sent)
  - `func gateway.NewCloudHub(b *Budget, tokens TokenSource, newAPI func(token string) CloudAPI, now func() time.Time, log *slog.Logger) *CloudHub`; `(*CloudHub).Locks(ctx, p Priority, notBefore time.Time) (LockList, error)`, `Command(ctx, lockID string, s loqed.BoltState) error`, `Token() string`, `Budget() *Budget`

Hub rules (spec §5.6): one `ListLocks` result serves all locks for 30 s, but only if it was fetched at or after `notBefore` (confirmation polls pass the command time). Budget is taken only once a client (token) exists, i.e. when a request is really sent. On `ErrUnauthorized` the hub asks the token source for a replacement and retries once (a rejected request did nothing). On `ErrRateLimited` it blocks the budget for 12 h. Reads are serialized with a context-aware semaphore; no mutex is held during network calls, and commands never wait for reads. Commands are recorded in the budget (`Record`) and logged with the window count.

- [ ] **Step 1: Write failing tests**

`internal/gateway/budget_test.go`:

```go
package gateway

import (
	"errors"
	"testing"
	"time"

	"github.com/t3hk0d3/go-loqed/internal/store"
)

func newBudget(limit int, now *time.Time) *Budget {
	return NewBudget(limit, 12*time.Hour, func() time.Time { return *now }, store.BudgetState{}, nil)
}

func TestBudgetLimitsCallsPerWindow(t *testing.T) {
	now := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	b := newBudget(3, &now)
	for i := range 3 {
		if err := b.Take(PriorityConfirm); err != nil {
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

func TestBudgetReservesPerPriority(t *testing.T) {
	now := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	b := newBudget(10, &now)
	for range 8 {
		if err := b.Take(PriorityConfirm); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Take(PriorityBackground); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("background must leave 2: %v", err)
	}
	if err := b.Take(PriorityRefresh); err != nil {
		t.Fatalf("refresh may use the 9th: %v", err)
	}
	if err := b.Take(PriorityRefresh); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("refresh must leave 1 for confirmations: %v", err)
	}
	if err := b.Take(PriorityConfirm); err != nil {
		t.Fatalf("confirm may use the last call: %v", err)
	}
}

func TestBudgetBackgroundSpacing(t *testing.T) {
	now := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	b := newBudget(10, &now)
	if err := b.Take(PriorityBackground); err != nil {
		t.Fatal(err)
	}
	if err := b.Take(PriorityBackground); !errors.Is(err, ErrDeferred) {
		t.Fatalf("second background poll within 72m must be deferred: %v", err)
	}
	now = now.Add(72 * time.Minute)
	if err := b.Take(PriorityBackground); err != nil {
		t.Fatal(err)
	}
	if b.Spacing() != 72*time.Minute {
		t.Fatalf("spacing %v", b.Spacing())
	}
}

func TestBudgetBlock(t *testing.T) {
	now := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	b := newBudget(10, &now)
	b.Block(12 * time.Hour)
	if err := b.Take(PriorityConfirm); !errors.Is(err, ErrCloudBlocked) {
		t.Fatalf("got %v", err)
	}
	now = now.Add(12 * time.Hour)
	if err := b.Take(PriorityConfirm); err != nil {
		t.Fatal(err)
	}
}

// Restarts must not reset the window: a crash loop would otherwise get the
// account blocked by LOQED.
func TestBudgetPersistsAcrossRestarts(t *testing.T) {
	now := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	var saved store.BudgetState
	save := func(s store.BudgetState) { saved = s }
	b := NewBudget(3, 12*time.Hour, func() time.Time { return now }, store.BudgetState{}, save)
	_ = b.Take(PriorityConfirm)
	_ = b.Take(PriorityConfirm)
	b.Block(time.Hour)
	if len(saved.Calls) != 2 || !saved.BlockedUntil.Equal(now.Add(time.Hour)) {
		t.Fatalf("saved %+v", saved)
	}
	restarted := NewBudget(3, 12*time.Hour, func() time.Time { return now }, saved, save)
	if err := restarted.Take(PriorityConfirm); !errors.Is(err, ErrCloudBlocked) {
		t.Fatalf("block lost on restart: %v", err)
	}
	now = now.Add(time.Hour)
	_ = restarted.Take(PriorityConfirm)
	if err := restarted.Take(PriorityConfirm); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("calls lost on restart: %v", err)
	}
}

func TestBudgetClampsFutureTimestamps(t *testing.T) {
	now := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	state := store.BudgetState{Calls: []time.Time{now.Add(48 * time.Hour)}, BlockedUntil: now.Add(100 * time.Hour)}
	b := NewBudget(3, 12*time.Hour, func() time.Time { return now }, state, nil)
	now = now.Add(12 * time.Hour)
	if b.Remaining() != 3 {
		t.Fatalf("a call stamped in the future must expire after one window: %d", b.Remaining())
	}
	if err := b.Take(PriorityConfirm); err != nil {
		t.Fatalf("block must be clamped to 12h: %v", err)
	}
}

func TestBudgetRecordCountsCommands(t *testing.T) {
	now := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	b := newBudget(10, &now)
	for range 9 {
		b.Record()
	}
	if n := b.Record(); n != 10 {
		t.Fatalf("count %d", n)
	}
	if err := b.Take(PriorityConfirm); !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("commands must count toward reads: %v", err)
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
	"github.com/t3hk0d3/go-loqed/internal/store"
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

type fakeTokens struct {
	token, next string
	err         error
}

func (f *fakeTokens) Token(context.Context) (string, error) { return f.token, f.err }
func (f *fakeTokens) Invalidate(_ context.Context, rejected string) (string, error) {
	if f.next == "" {
		return "", loqed.ErrUnauthorized
	}
	f.token = f.next
	return f.next, nil
}

func newHub(now *time.Time, tokens TokenSource, apis map[string]*scriptedAPI) *CloudHub {
	clock := func() time.Time { return *now }
	return NewCloudHub(NewBudget(10, 12*time.Hour, clock, store.BudgetState{}, nil), tokens, func(tok string) CloudAPI {
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
		if _, err := h.Locks(ctx, PriorityRefresh, time.Time{}); err != nil {
			t.Fatal(err)
		}
	}
	if api.calls != 1 {
		t.Fatalf("calls %d", api.calls)
	}
	now = now.Add(31 * time.Second)
	list, _ := h.Locks(ctx, PriorityRefresh, time.Time{})
	if api.calls != 2 || h.Token() != "tok" || !list.FetchedAt.Equal(now) {
		t.Fatalf("calls %d token %q fetched %v", api.calls, h.Token(), list.FetchedAt)
	}
}

// A confirmation poll must never be answered from data fetched before the
// command (that showed LOCKED for an hour after an UNLOCK).
func TestHubConfirmIgnoresOlderCachedResult(t *testing.T) {
	now := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	api := &scriptedAPI{}
	h := newHub(&now, &fakeTokens{token: "tok"}, map[string]*scriptedAPI{"tok": api})
	ctx := context.Background()
	_, _ = h.Locks(ctx, PriorityBackground, time.Time{})
	now = now.Add(10 * time.Second)
	commandAt := now
	now = now.Add(5 * time.Second)
	list, err := h.Locks(ctx, PriorityConfirm, commandAt)
	if err != nil || api.calls != 2 || list.FetchedAt.Before(commandAt) {
		t.Fatalf("calls %d fetched %v err %v", api.calls, list.FetchedAt, err)
	}
}

func TestHubTakesBudgetOnlyWhenSending(t *testing.T) {
	now := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	tokens := &fakeTokens{err: errors.New("mint refused")}
	h := newHub(&now, tokens, map[string]*scriptedAPI{})
	for range 5 {
		_, _ = h.Locks(context.Background(), PriorityConfirm, time.Time{})
	}
	if h.Budget().Remaining() != 10 {
		t.Fatalf("budget spent without requests: %d left", h.Budget().Remaining())
	}
}

func TestHubReauthenticatesOnce(t *testing.T) {
	now := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	old := &scriptedAPI{errs: []error{loqed.ErrUnauthorized}}
	fresh := &scriptedAPI{}
	h := newHub(&now, &fakeTokens{token: "old", next: "new"}, map[string]*scriptedAPI{"old": old, "new": fresh})
	list, err := h.Locks(context.Background(), PriorityRefresh, time.Time{})
	if err != nil || len(list.Locks) != 1 || fresh.calls != 1 || h.Token() != "new" {
		t.Fatalf("locks %v err %v fresh %d token %q", list, err, fresh.calls, h.Token())
	}
}

func TestHubRateLimitBlocksBudget(t *testing.T) {
	now := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	api := &scriptedAPI{errs: []error{loqed.ErrRateLimited}}
	h := newHub(&now, &fakeTokens{token: "tok"}, map[string]*scriptedAPI{"tok": api})
	if _, err := h.Locks(context.Background(), PriorityRefresh, time.Time{}); !errors.Is(err, loqed.ErrRateLimited) {
		t.Fatalf("got %v", err)
	}
	now = now.Add(time.Hour)
	if _, err := h.Locks(context.Background(), PriorityRefresh, time.Time{}); !errors.Is(err, ErrCloudBlocked) {
		t.Fatalf("got %v", err)
	}
	if api.calls != 1 {
		t.Fatalf("blocked hub must not call the API: %d", api.calls)
	}
}

func TestHubCommandReauthenticatesAndIsRecorded(t *testing.T) {
	now := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	old, fresh := &scriptedAPI{}, &scriptedAPI{}
	h := newHub(&now, &fakeTokens{token: "old", next: "new"}, map[string]*scriptedAPI{"old": old, "new": fresh})
	if err := h.Command(context.Background(), "lock1", loqed.BoltNightLock); err != nil {
		t.Fatal(err)
	}
	if old.commands != 1 || fresh.commands != 1 || h.Budget().Remaining() != 8 {
		t.Fatalf("old %d fresh %d remaining %d", old.commands, fresh.commands, h.Budget().Remaining())
	}
}

// A slow read must not hold up a door command.
func TestHubCommandDoesNotWaitForReads(t *testing.T) {
	now := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	api := &scriptedAPI{}
	h := newHub(&now, &fakeTokens{token: "tok"}, map[string]*scriptedAPI{"tok": api})
	h.reads <- struct{}{} // a read is in progress
	defer func() { <-h.reads }()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := h.Command(ctx, "lock1", loqed.BoltOpen); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Locks(ctx, PriorityConfirm, time.Time{}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("reads wait for the in-flight read, bounded by ctx: %v", err)
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/gateway/`
Expected: FAIL — `no non-test Go files in …/internal/gateway`.

- [ ] **Step 3: Implement `budget.go`**

`internal/gateway/budget.go`:

```go
// Package gateway runs one supervisor per lock: local-first operation,
// cloud fallback, offline handling, and the shared cloud request budget.
package gateway

import (
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/t3hk0d3/go-loqed/internal/store"
)

type Priority int

const (
	PriorityConfirm    Priority = iota // confirm a command sent via cloud; may use the whole budget
	PriorityRefresh                    // credential refresh; leaves 1 call for confirmations
	PriorityBackground                 // periodic cloud-mode poll; leaves 2 and is spaced out
)

var (
	ErrBudgetExhausted = errors.New("gateway: cloud request budget exhausted")
	ErrDeferred        = errors.New("gateway: background cloud poll deferred to spread the budget")
	ErrCloudBlocked    = errors.New("gateway: cloud reads suspended after LOQED rate limiting")
)

// reserve is how many calls each priority must leave unused.
func reserve(p Priority, limit int) int {
	switch p {
	case PriorityRefresh:
		return min(1, limit-1)
	case PriorityBackground:
		return min(2, limit-1)
	default:
		return 0
	}
}

// Budget limits cloud calls per rolling window, account-wide. Its state is
// persisted through save after every change, so restarts (crash loops,
// watchdogs) cannot exceed LOQED's account limit.
type Budget struct {
	limit  int
	window time.Duration
	now    func() time.Time
	save   func(store.BudgetState) // may be nil

	mu             sync.Mutex
	calls          []time.Time
	lastBackground time.Time
	blockedUntil   time.Time
}

// NewBudget restores state (timestamps in the future are clamped to now,
// a block to at most RateLimitBackoff from now).
func NewBudget(limit int, window time.Duration, now func() time.Time, state store.BudgetState, save func(store.BudgetState)) *Budget {
	b := &Budget{limit: limit, window: window, now: now, save: save}
	t := now()
	for _, c := range state.Calls {
		if c.After(t) {
			c = t
		}
		b.calls = append(b.calls, c)
	}
	slices.SortFunc(b.calls, func(a, c time.Time) int { return a.Compare(c) })
	if n := len(b.calls); n > 0 {
		b.lastBackground = b.calls[n-1] // keep spacing across restarts
	}
	b.blockedUntil = state.BlockedUntil
	if limitUntil := t.Add(RateLimitBackoff); b.blockedUntil.After(limitUntil) {
		b.blockedUntil = limitUntil
	}
	b.prune(t)
	return b
}

// Take reserves one read call for priority p.
func (b *Budget) Take(p Priority) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	if now.Before(b.blockedUntil) {
		return ErrCloudBlocked
	}
	b.prune(now)
	remaining := b.limit - len(b.calls)
	if remaining <= reserve(p, b.limit) {
		return ErrBudgetExhausted
	}
	if p == PriorityBackground {
		spacing := b.window / time.Duration(b.limit)
		if !b.lastBackground.IsZero() && now.Sub(b.lastBackground) < spacing {
			return ErrDeferred
		}
		b.lastBackground = now
	}
	b.calls = append(b.calls, now)
	b.persistLocked()
	return nil
}

// Record counts a call that must not be refused (a door command) and
// returns how many calls the window now holds.
func (b *Budget) Record() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	b.prune(now)
	b.calls = append(b.calls, now)
	b.persistLocked()
	return len(b.calls)
}

func (b *Budget) Block(d time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.blockedUntil = b.now().Add(d)
	b.persistLocked()
}

func (b *Budget) Remaining() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.prune(b.now())
	return b.limit - len(b.calls)
}

// Spacing is the interval between background polls.
func (b *Budget) Spacing() time.Duration { return b.window / time.Duration(b.limit) }

func (b *Budget) prune(now time.Time) {
	cut := 0
	for cut < len(b.calls) && now.Sub(b.calls[cut]) >= b.window {
		cut++
	}
	b.calls = b.calls[cut:]
}

func (b *Budget) persistLocked() {
	if b.save != nil {
		b.save(store.BudgetState{Calls: slices.Clone(b.calls), BlockedUntil: b.blockedUntil})
	}
}
```

- [ ] **Step 4: Implement `cloudhub.go`**

`internal/gateway/cloudhub.go`:

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

// LockList is one GET /api/locks/ result. FetchedAt is when the request
// was sent: the data is at least that fresh.
type LockList struct {
	Locks     []cloud.Lock
	FetchedAt time.Time
}

// CloudHub serializes cloud reads: one ListLocks result serves every lock,
// reads are budgeted, and rejected tokens are replaced once. Commands do
// not wait for reads; no lock is held during network calls.
type CloudHub struct {
	budget *Budget
	tokens TokenSource
	newAPI func(token string) CloudAPI
	now    func() time.Time
	log    *slog.Logger

	reads chan struct{} // one ListLocks at a time (ctx-aware)

	mu    sync.Mutex
	api   CloudAPI
	token string
	last  LockList
}

func NewCloudHub(b *Budget, tokens TokenSource, newAPI func(token string) CloudAPI, now func() time.Time, log *slog.Logger) *CloudHub {
	return &CloudHub{budget: b, tokens: tokens, newAPI: newAPI, now: now, log: log, reads: make(chan struct{}, 1)}
}

// Token is the token currently in use ("" before the first call).
func (h *CloudHub) Token() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.token
}

// Budget exposes the shared budget (for spacing and diagnostics).
func (h *CloudHub) Budget() *Budget { return h.budget }

// Locks returns the account's locks. A result fetched within the last 30 s
// is shared, but only if it was fetched at or after notBefore (pass the
// command time for confirmation polls, zero otherwise). Callers must not
// modify the slice.
func (h *CloudHub) Locks(ctx context.Context, p Priority, notBefore time.Time) (LockList, error) {
	select {
	case h.reads <- struct{}{}:
	case <-ctx.Done():
		return LockList{}, ctx.Err()
	}
	defer func() { <-h.reads }()

	h.mu.Lock()
	last := h.last
	h.mu.Unlock()
	now := h.now()
	if last.Locks != nil && now.Sub(last.FetchedAt) < coalesceWindow && !last.FetchedAt.Before(notBefore) {
		return last, nil
	}
	api, tok, err := h.client(ctx)
	if err != nil {
		return LockList{}, err
	}
	if err := h.budget.Take(p); err != nil {
		return LockList{}, err
	}
	started := h.now()
	locks, err := listLocks(ctx, api)
	if errors.Is(err, loqed.ErrUnauthorized) {
		api, rerr := h.reauth(ctx, tok)
		if rerr != nil {
			return LockList{}, errors.Join(err, rerr)
		}
		if err = h.budget.Take(p); err == nil {
			started = h.now()
			locks, err = listLocks(ctx, api)
		}
	}
	if errors.Is(err, loqed.ErrRateLimited) {
		h.budget.Block(RateLimitBackoff)
		h.log.Error("LOQED cloud rate limit reached; cloud reads suspended for 12h", "err", err)
	}
	if err != nil {
		return LockList{}, err
	}
	res := LockList{Locks: locks, FetchedAt: started}
	h.mu.Lock()
	h.last = res
	h.mu.Unlock()
	return res, nil
}

// Command sends a door command. It is never refused by the budget but is
// recorded in it (pending V2: LOQED may count commands too). A 401 is
// retried once with a replacement token: a rejected request did nothing.
func (h *CloudHub) Command(ctx context.Context, lockID string, s loqed.BoltState) error {
	api, tok, err := h.client(ctx)
	if err != nil {
		return err
	}
	err = h.command(ctx, api, lockID, s)
	if errors.Is(err, loqed.ErrUnauthorized) {
		api, rerr := h.reauth(ctx, tok)
		if rerr != nil {
			return errors.Join(err, rerr)
		}
		err = h.command(ctx, api, lockID, s)
	}
	return err
}

func (h *CloudHub) command(ctx context.Context, api CloudAPI, lockID string, s loqed.BoltState) error {
	n := h.budget.Record()
	h.log.Info("sending cloud command", "lock_id", lockID, "state", s, "cloud_calls_in_window", n)
	ctx, cancel := context.WithTimeout(ctx, cloudTimeout)
	defer cancel()
	return api.Command(ctx, lockID, s)
}

func (h *CloudHub) client(ctx context.Context) (CloudAPI, string, error) {
	h.mu.Lock()
	api, tok := h.api, h.token
	h.mu.Unlock()
	if api != nil {
		return api, tok, nil
	}
	tok, err := h.tokens.Token(ctx) // the resolver serializes minting
	if err != nil {
		return nil, "", err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.api == nil || h.token != tok {
		h.token, h.api = tok, h.newAPI(tok)
	}
	return h.api, h.token, nil
}

func (h *CloudHub) reauth(ctx context.Context, rejected string) (CloudAPI, error) {
	tok, err := h.tokens.Invalidate(ctx, rejected)
	if err != nil {
		return nil, err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.token != tok {
		h.token, h.api = tok, h.newAPI(tok)
	}
	return h.api, nil
}

func listLocks(ctx context.Context, api CloudAPI) ([]cloud.Lock, error) {
	ctx, cancel := context.WithTimeout(ctx, cloudTimeout)
	defer cancel()
	return api.ListLocks(ctx)
}
```

- [ ] **Step 5: Run tests**

Run: `gofmt -l internal/gateway && go test ./internal/gateway/ -v -race`
Expected: PASS (14 tests).

- [ ] **Step 6: Commit**

```bash
git add internal/gateway
git commit -m "gateway: add the persisted cloud budget and cloud hub

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 8: Credential refresher and lock records

**Files:**
- Create: `internal/gateway/refresher.go`, `internal/gateway/records.go`, `internal/gateway/refresher_test.go`

**Interfaces:**
- Consumes: `CloudHub` (via `LockLister`), `LockList`, `store.Store`, `store.FromCloud`, `store.Merge`, `store.SameLocal`, `store.TokenHash`, `config.LockSetting`, `config.LockSettingsMap`.
- Produces:
  - `type gateway.Reason string`: `ReasonUnauthorized="unauthorized"`, `ReasonUnreachable="unreachable"`
  - `var gateway.ErrRefreshThrottled, ErrEmptyLockList`
  - `type gateway.LockLister interface{ Locks(ctx, Priority, notBefore time.Time) (LockList, error); Token() string }`
  - `func gateway.NewRefresher(c LockLister, st *store.Store, now func() time.Time) *Refresher`; field `OnRemoved func(ids []string)` (called when a refresh drops locks from the cache)
  - `(*Refresher).RefreshAll(ctx) ([]store.LockRecord, error)` — merges per lock (`store.Merge`), removes locks absent from a non-empty list, refuses an empty list while the cache has locks (`ErrEmptyLockList`); a `store.ErrWrite` still returns the fresh records
  - `(*Refresher).Refresh(ctx, lockID string, reason Reason) (store.LockRecord, error)` — per lock+reason backoff: 5 min, doubling to 6 h while the lock's local credentials come back unchanged; a change resets it
  - `(*Refresher).RefreshIfOlder(ctx, maxAge time.Duration) (bool, error)` (`cache_max_age` at runtime)
  - `func gateway.SettingFor(settings config.LockSettingsMap, rec store.LockRecord) config.LockSetting` (by id, then name); `func gateway.ApplySetting(rec store.LockRecord, s config.LockSetting) store.LockRecord`
  - `func gateway.KeysPinned(config.LockSetting) bool` (bridge_key, key_secret or local_id set), `func gateway.IPPinned(config.LockSetting) bool`
  - `func gateway.Select(records []store.LockRecord, allow []string) (selected []store.LockRecord, missing []string)`; `func gateway.UnmatchedSettings(settings config.LockSettingsMap, records []store.LockRecord) []string`

- [ ] **Step 1: Write failing tests**

`internal/gateway/refresher_test.go`:

```go
package gateway

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/t3hk0d3/go-loqed/cloud"
	"github.com/t3hk0d3/go-loqed/internal/config"
	"github.com/t3hk0d3/go-loqed/internal/store"
)

type listerStub struct {
	locks []cloud.Lock
	calls int
	err   error
}

func (l *listerStub) Locks(context.Context, Priority, time.Time) (LockList, error) {
	l.calls++
	return LockList{Locks: l.locks}, l.err
}
func (l *listerStub) Token() string { return "tok" }

func cloudLock(id, ip string) cloud.Lock {
	lid := 1
	return cloud.Lock{ID: id, Name: "Lock " + id, BridgeIP: ip, LocalID: &lid, KeySecret: "k", BridgeKey: "b"}
}

func newStore(t *testing.T) *store.Store {
	t.Helper()
	st, _, err := store.Open(filepath.Join(t.TempDir(), "locks.json"))
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func TestRefreshAllWritesCache(t *testing.T) {
	st := newStore(t)
	l := &listerStub{locks: []cloud.Lock{cloudLock("lock1", "192.0.2.4")}}
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

func TestRefreshAllMergesAndRemoves(t *testing.T) {
	st := newStore(t)
	now := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	l := &listerStub{locks: []cloud.Lock{cloudLock("a", "192.0.2.4"), cloudLock("b", "192.0.2.5")}}
	r := NewRefresher(l, st, func() time.Time { return now })
	var removed []string
	r.OnRemoved = func(ids []string) { removed = ids }
	_, _ = r.RefreshAll(context.Background())

	// The cloud stops reporting local fields for "a" and drops "b".
	l.locks = []cloud.Lock{{ID: "a", Name: "Renamed"}}
	recs, err := r.RefreshAll(context.Background())
	if err != nil || len(recs) != 1 || recs[0].Name != "Renamed" || !recs[0].HasLocalCredentials() || recs[0].BridgeIP != "192.0.2.4" {
		t.Fatalf("%+v %v", recs, err)
	}
	if !slices.Equal(removed, []string{"b"}) {
		t.Fatalf("removed %v", removed)
	}
}

func TestRefreshAllIgnoresEmptyList(t *testing.T) {
	st := newStore(t)
	l := &listerStub{locks: []cloud.Lock{cloudLock("a", "192.0.2.4")}}
	r := NewRefresher(l, st, time.Now)
	_, _ = r.RefreshAll(context.Background())
	l.locks = nil
	if _, err := r.RefreshAll(context.Background()); !errors.Is(err, ErrEmptyLockList) {
		t.Fatalf("got %v", err)
	}
	if len(st.Snapshot().Locks) != 1 {
		t.Fatal("an empty list must not wipe the cache")
	}
}

func TestRefreshBacksOffWhileNothingChanges(t *testing.T) {
	st := newStore(t)
	l := &listerStub{locks: []cloud.Lock{cloudLock("lock1", "192.0.2.4")}}
	now := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	r := NewRefresher(l, st, func() time.Time { return now })
	ctx := context.Background()
	_, _ = r.RefreshAll(ctx) // cache primed
	// Same data every time: 5m, 10m, 20m, 40m ... capped at 6h.
	var allowed []time.Duration
	start := now
	for now.Sub(start) < 12*time.Hour {
		if _, err := r.Refresh(ctx, "lock1", ReasonUnreachable); err == nil {
			allowed = append(allowed, now.Sub(start))
		} else if !errors.Is(err, ErrRefreshThrottled) {
			t.Fatal(err)
		}
		now = now.Add(time.Minute)
	}
	if len(allowed) > 8 {
		t.Fatalf("a flapping bridge spent %d refreshes in 12h: %v", len(allowed), allowed)
	}
	if allowed[1]-allowed[0] != 10*time.Minute {
		t.Fatalf("second gap %v", allowed[1]-allowed[0])
	}
	// A different reason has its own schedule.
	if _, err := r.Refresh(ctx, "lock1", ReasonUnauthorized); err != nil {
		t.Fatalf("different reason must not be throttled: %v", err)
	}
	if _, err := r.Refresh(ctx, "missing", ReasonUnreachable); err == nil {
		t.Fatal("expected error for lock no longer on the account")
	}
}

func TestRefreshBackoffResetsWhenDataChanges(t *testing.T) {
	st := newStore(t)
	l := &listerStub{locks: []cloud.Lock{cloudLock("lock1", "192.0.2.4")}}
	now := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	r := NewRefresher(l, st, func() time.Time { return now })
	ctx := context.Background()
	_, _ = r.RefreshAll(ctx)
	_, _ = r.Refresh(ctx, "lock1", ReasonUnreachable) // unchanged → next in 10m
	now = now.Add(10 * time.Minute)
	l.locks[0].BridgeIP = "192.0.2.9"
	if rec, err := r.Refresh(ctx, "lock1", ReasonUnreachable); err != nil || rec.BridgeIP != "192.0.2.9" {
		t.Fatalf("%+v %v", rec, err)
	}
	now = now.Add(5 * time.Minute)
	if _, err := r.Refresh(ctx, "lock1", ReasonUnreachable); err != nil {
		t.Fatalf("a change must reset the backoff to 5m: %v", err)
	}
}

func TestRefreshIfOlder(t *testing.T) {
	st := newStore(t)
	l := &listerStub{locks: []cloud.Lock{cloudLock("lock1", "192.0.2.4")}}
	now := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	r := NewRefresher(l, st, func() time.Time { return now })
	_, _ = r.RefreshAll(context.Background())
	if ran, _ := r.RefreshIfOlder(context.Background(), 0); ran {
		t.Fatal("0 disables age refreshes")
	}
	now = now.Add(7 * 24 * time.Hour)
	if ran, err := r.RefreshIfOlder(context.Background(), 168*time.Hour); !ran || err != nil || l.calls != 2 {
		t.Fatalf("ran %v err %v calls %d", ran, err, l.calls)
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
	settings := config.LockSettingsMap{"Front door": {BridgeIP: "10.0.0.9", LocalID: &id, KeyNames: config.KeyNames{1: "Alice"}}, "Shed": {}}
	s := SettingFor(settings, recs[0])
	got := ApplySetting(recs[0], s)
	if got.BridgeIP != "10.0.0.9" || *got.LocalID != 7 || got.Name != "Front door" {
		t.Fatalf("%+v", got)
	}
	if SettingFor(settings, recs[1]).BridgeIP != "" {
		t.Fatal("no setting expected for b")
	}
	if !KeysPinned(s) || !IPPinned(s) || KeysPinned(config.LockSetting{BridgeIP: "x"}) {
		t.Fatal("pinning helpers")
	}
	if u := UnmatchedSettings(settings, recs); !slices.Equal(u, []string{"Shed"}) {
		t.Fatalf("unmatched %v", u)
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/gateway/ -run 'Refresh|Select'`
Expected: FAIL — `undefined: NewRefresher`.

- [ ] **Step 3: Implement `refresher.go`**

`internal/gateway/refresher.go`:

```go
package gateway

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/t3hk0d3/go-loqed/internal/store"
)

type Reason string

const (
	ReasonUnauthorized Reason = "unauthorized"
	ReasonUnreachable  Reason = "unreachable"
)

var (
	ErrRefreshThrottled = errors.New("gateway: credential refresh throttled")
	ErrEmptyLockList    = errors.New("gateway: the cloud returned no locks; keeping the cached lock data")
)

const (
	refreshBackoffMin = 5 * time.Minute
	refreshBackoffMax = 6 * time.Hour
)

type LockLister interface {
	Locks(ctx context.Context, p Priority, notBefore time.Time) (LockList, error)
	Token() string
}

type backoff struct {
	next     time.Time
	interval time.Duration
}

// Refresher reloads lock credentials from the cloud into the store.
type Refresher struct {
	cloud LockLister
	store *store.Store
	now   func() time.Time

	// OnRemoved, if set, is called with the ids of locks that a refresh
	// removed from the cache (no longer on the account).
	OnRemoved func(ids []string)

	mu    sync.Mutex
	state map[string]*backoff // per lock+reason
}

func NewRefresher(c LockLister, st *store.Store, now func() time.Time) *Refresher {
	return &Refresher{cloud: c, store: st, now: now, state: map[string]*backoff{}}
}

// RefreshAll fetches the lock list and merges it into the cache: records
// keep cached local credentials the cloud no longer reports, locks absent
// from a non-empty list are removed, and an empty list is not applied
// while the cache has locks.
func (r *Refresher) RefreshAll(ctx context.Context) ([]store.LockRecord, error) {
	list, err := r.cloud.Locks(ctx, PriorityRefresh, time.Time{})
	if err != nil {
		return nil, err
	}
	before := r.store.Snapshot()
	if len(list.Locks) == 0 && len(before.Locks) > 0 {
		return nil, ErrEmptyLockList
	}
	recs := make([]store.LockRecord, 0, len(list.Locks))
	kept := map[string]bool{}
	for _, l := range list.Locks {
		rec := store.FromCloud(l)
		if old, ok := before.Find(l.ID); ok && old.ID == l.ID {
			rec = store.Merge(old, rec)
		}
		recs = append(recs, rec)
		kept[rec.ID] = true
	}
	var removed []string
	for _, old := range before.Locks {
		if !kept[old.ID] {
			removed = append(removed, old.ID)
		}
	}
	err = r.store.Update(func(c *store.Cache) {
		c.Locks = recs
		c.FetchedAt = r.now().UTC()
		c.TokenSHA256 = store.TokenHash(r.cloud.Token())
	})
	if len(removed) > 0 && r.OnRemoved != nil {
		r.OnRemoved(removed)
	}
	return recs, err // a store.ErrWrite still returns the fresh records
}

// Refresh refreshes for one lock and reason. Attempts back off
// exponentially (5 min doubling to 6 h) while refreshes return the same
// local credentials for the lock; a change resets the backoff.
func (r *Refresher) Refresh(ctx context.Context, lockID string, reason Reason) (store.LockRecord, error) {
	key := lockID + "/" + string(reason)
	now := r.now()
	r.mu.Lock()
	b := r.state[key]
	if b == nil {
		b = &backoff{interval: refreshBackoffMin}
		r.state[key] = b
	}
	if now.Before(b.next) {
		r.mu.Unlock()
		return store.LockRecord{}, ErrRefreshThrottled
	}
	b.next = now.Add(b.interval)
	r.mu.Unlock()

	old, _ := r.store.Snapshot().Find(lockID)
	recs, err := r.RefreshAll(ctx)
	if recs == nil {
		return store.LockRecord{}, err
	}
	for _, rec := range recs {
		if rec.ID != lockID {
			continue
		}
		r.mu.Lock()
		if store.SameLocal(old, rec) {
			b.interval = min(2*b.interval, refreshBackoffMax)
		} else {
			b.interval = refreshBackoffMin
		}
		b.next = now.Add(b.interval)
		r.mu.Unlock()
		return rec, nil
	}
	return store.LockRecord{}, fmt.Errorf("gateway: lock %s is no longer on the account", lockID)
}

// RefreshIfOlder refreshes when cache_max_age (> 0) has passed since the
// last fetch. It reports whether a refresh ran.
func (r *Refresher) RefreshIfOlder(ctx context.Context, maxAge time.Duration) (bool, error) {
	if maxAge <= 0 || r.now().Sub(r.store.Snapshot().FetchedAt) < maxAge {
		return false, nil
	}
	_, err := r.RefreshAll(ctx)
	return true, err
}
```

- [ ] **Step 4: Implement `records.go`**

`internal/gateway/records.go`:

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

// KeysPinned: lock_settings overrides the bridge keys, so a cloud refresh
// cannot fix an auth failure.
func KeysPinned(s config.LockSetting) bool {
	return s.BridgeKey != "" || s.KeySecret != "" || s.LocalID != nil
}

// IPPinned: lock_settings overrides the bridge IP, so a cloud refresh
// cannot fix an unreachable bridge.
func IPPinned(s config.LockSetting) bool { return s.BridgeIP != "" }

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

// UnmatchedSettings lists lock_settings keys that match no lock id or name
// (logged as warnings).
func UnmatchedSettings(settings config.LockSettingsMap, records []store.LockRecord) []string {
	var out []string
	for key := range settings {
		found := false
		for _, r := range records {
			if r.ID == key || r.Name == key {
				found = true
				break
			}
		}
		if !found {
			out = append(out, key)
		}
	}
	return out
}
```

- [ ] **Step 5: Run tests**

Run: `gofmt -l internal/gateway && go test ./internal/gateway/ -v -race`
Expected: PASS (21 tests).

- [ ] **Step 6: Commit**

```bash
git add internal/gateway
git commit -m "gateway: add credential refresh with merge and backoff, lock records

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 9: Supervisor core and local mode

**Files:**
- Create: `internal/gateway/supervisor.go`, `internal/gateway/local.go`, `internal/gateway/harness_test.go`, `internal/gateway/local_test.go`
- Create (placeholders, each replaced whole by a later task): `internal/gateway/cloudmode.go` (Task 10), `internal/gateway/commands.go` (Task 11), `internal/gateway/cloudevents.go` (Task 12)

**Interfaces:**
- Consumes: `bridge.*` types, `model.*`, `store.LockRecord`, `config.LockSetting`, `ApplySetting`, `KeysPinned`, `IPPinned`, `Priority`, `Reason`, `LockList`, `ErrRefreshThrottled`.
- Produces:
  - `type gateway.BridgeAPI interface{ Status(ctx) (*bridge.Status, error); Command(ctx, bridge.Action) error; ListWebhooks(ctx) ([]bridge.Webhook, error); CreateWebhook(ctx, url string, t bridge.Triggers) error; DeleteWebhook(ctx, id int) error }` (satisfied by `*bridge.Client`)
  - `type gateway.CloudSource interface{ Locks(ctx, Priority, notBefore time.Time) (LockList, error); Command(ctx, lockID string, s loqed.BoltState) error }` (satisfied by `*CloudHub`)
  - `type gateway.Publisher interface{ PublishState(id string, s model.State) error; PublishEvent(id string, e model.Event) error; PublishAvailability(id string, online bool) error }` (satisfied by `*hass.Client`)
  - `type gateway.Prober func(ctx context.Context, address string) error`; `func gateway.TCPProbe(ctx, address string) error`; `func gateway.BridgeAddress(bridgeIP string) string` (`ip` → `ip:80`; keeps an explicit `host:port`)
  - Messages: `gateway.BridgeEventMsg{Event bridge.Event}`, `gateway.CloudEventMsg{Event cloud.WebhookEvent}`, `gateway.CommandMsg{Command model.Command; At time.Time}`
  - `type gateway.Deps struct{ Publisher Publisher; Cloud CloudSource; Refresh func(ctx, lockID string, reason Reason) (store.LockRecord, error); NewBridge func(store.LockRecord) (BridgeAPI, error); Probe Prober; ProbeCloud func(ctx) error; WebhookURL func(store.LockRecord) (string, error); CloudWebhooks bool; Now func() time.Time; Log *slog.Logger }`
  - `type gateway.Timing struct{ Liveness, Reconcile, OfflineRetry, UnknownRecheck, WebhookConfirm, WebhookRetry, CloudConfirm, CloudPoll, CloudPollSpacing, StaleGrace, CommandMaxAge, EnrichWindow, RequestTimeout time.Duration; FailureThreshold int }`; `func gateway.DefaultTiming(liveness, reconcile, pollSpacing time.Duration) Timing`
  - `func gateway.NewSupervisor(rec store.LockRecord, setting config.LockSetting, d Deps, t Timing) *Supervisor`; methods `ID() string`, `Record() store.LockRecord`, `BridgeKey() ([]byte, bool)`, `Deliver(msg any) bool`, `Health() Health`, `Run(ctx)`
  - `type gateway.Health struct{ Mode model.Mode; Available bool; LastEventAt *time.Time }` (JSON `mode`, `available`, `last_event_at`)
  - Unexported, used by later tasks: `start`, `tick`, `handle`, `publish`, `setMode`, `setRecord`, `markStale`, `warn` (rate-limited warnings), `recordEvent`, `onReached`, `awaitConfirm`, `scheduleCloudConfirm`, `commandFailed`, `failClass`, `keyName`, `reqCtx`, `available`, `canLocal`, `ensureBridge`, `tryEnterLocal`, `status`, `httpFailure`, `refreshAndRebuild`, `errNoBridge`; fields `mode`, `state`, `bridge`, `webhookOK`, failure counters `probeFailures`/`httpFailures`/`cloudProbeFailures`/`cloudAPIFailures`, schedule fields `nextProbe`, `nextCloudProbe`, `nextReconcile`, `nextCloudPoll`, `nextOfflineRetry`, confirmation fields `confirmTarget`, `confirmAt`, `confirmViaCloud`, `cloudConfirmAt`, `cloudConfirmSince`, and `lastFreshAt`, `lastEventAt`, `lastPollAt`, `lastBridgeEvent`, `lastCloudEventAt`.

Key behavior (spec §5.5):
- `s.bridge` is nil whenever no client can be built; every bridge call goes through `status`/`ensureWebhook`/`bridgeCommand`, which return `errNoBridge` instead of dereferencing nil, and `tickLocal` leaves local mode when the client cannot be rebuilt.
- TCP probe failures and HTTP failures are counted separately; a successful probe never resets the HTTP counter.
- A `GO_TO_STATE_*` event (or a command) arms a 10 s `/status` confirmation that only a `STATE_CHANGED_*` reaching the target cancels; `MOTOR_STALL` schedules one `/status` 10 s later.
- Failed webhook registration is retried every 10 min (with a `/status` poll each time) until it works.
- A signed bridge event is applied even if entering local mode fails.
- Credential refreshes are skipped when `lock_settings` pins what they would change.

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
	listCalls   int
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

func (f *fakeBridge) ListWebhooks(context.Context) ([]bridge.Webhook, error) {
	f.listCalls++
	return f.hooks, f.listErr
}

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
	states []model.State
	events []model.Event
	avail  []bool
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
```

- [ ] **Step 2: Write failing local-mode tests**

`internal/gateway/local_test.go`:

```go
package gateway

import (
	"errors"
	"testing"
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/bridge"
	"github.com/t3hk0d3/go-loqed/internal/config"
	"github.com/t3hk0d3/go-loqed/internal/model"
)

func reached(eventType string, key *int) BridgeEventMsg {
	b, jammed := loqed.ReachedState(eventType)
	return BridgeEventMsg{Event: bridge.StateReachedEvent{EventType: eventType, BoltState: b, Jammed: jammed, KeyLocalID: key}}
}

func goTo(eventType string, target loqed.BoltState) BridgeEventMsg {
	return BridgeEventMsg{Event: bridge.GoToStateEvent{EventType: eventType, GoToState: target, KeyLocalID: model.Ptr(255)}}
}

func TestStartsLocalAndRegistersWebhook(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	if h.s.mode != model.ModeLocal || h.lock() != "UNLOCKED" || !h.available() || h.state().Mode != model.ModeLocal {
		t.Fatalf("mode %s lock %s", h.s.mode, h.lock())
	}
	if len(h.bridge.created) != 1 || h.bridge.created[0] != "http://10.0.0.5:8099/webhook/lock1" || !h.s.webhookOK {
		t.Fatalf("created %v", h.bridge.created)
	}
	if h.state().BatteryPercentage == nil || *h.state().BatteryPercentage != 80 || h.state().StateStale {
		t.Fatalf("state %+v", h.state())
	}
}

func TestKeepsCurrentWebhookAndDeletesStaleOnes(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.bridge.hooks = []bridge.Webhook{
		{ID: 1, URL: "http://10.0.0.9:8099/webhook/lock1"},      // old gateway IP
		{ID: 2, URL: "http://10.0.0.5:8099/webhook/lock1"},      // current
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
	h.send(goTo("GO_TO_STATE_TOUCH_TO_LOCK", loqed.BoltNightLock))
	if h.lock() != "LOCKING" || h.pub.events[0].EventType != model.EventLocking || h.pub.events[0].Source != "touch" {
		t.Fatalf("lock %s events %+v", h.lock(), h.pub.events)
	}
	h.send(reached("STATE_CHANGED_NIGHT_LOCK", model.Ptr(255)))
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
	h.send(reached("STATE_CHANGED_LATCH", model.Ptr(3)))
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

func TestPinnedKeysSkipAuthRefresh(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{BridgeKey: "Ym9uam91ciBtb25kZQ=="})
	h.bridge.listErr = loqed.ErrUnauthorized
	h.start()
	if len(h.refreshes) != 0 {
		t.Fatalf("a refresh cannot fix pinned keys: %v", h.refreshes)
	}
}

// A STATE_CHANGED webhook can be lost; GO_TO_STATE alone must not leave
// HA showing LOCKING until the daily reconcile.
func TestLostStateChangedAfterGoToTriggersStatus(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.bridge.status.BoltState = loqed.BoltNightLock
	h.send(goTo("GO_TO_STATE_TOUCH_TO_LOCK", loqed.BoltNightLock))
	if h.lock() != "LOCKING" {
		t.Fatalf("lock %s", h.lock())
	}
	h.run(9 * time.Second)
	if h.bridge.statusCalls != 1 {
		t.Fatal("too early")
	}
	h.run(2 * time.Second)
	if h.bridge.statusCalls != 2 || h.lock() != "LOCKED" {
		t.Fatalf("status %d lock %s", h.bridge.statusCalls, h.lock())
	}
}

func TestMotorStallSchedulesStatus(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.send(reached("MOTOR_STALL", model.Ptr(1)))
	if h.lock() != "JAMMED" {
		t.Fatalf("lock %s", h.lock())
	}
	h.run(11 * time.Second)
	if h.bridge.statusCalls != 2 || h.lock() != "UNLOCKED" {
		t.Fatalf("status %d lock %s", h.bridge.statusCalls, h.lock())
	}
}

func TestWebhookRegistrationRetriedEveryTenMinutes(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.bridge.listErr = errors.New("bridge has no free webhook slots")
	h.start()
	if h.s.mode != model.ModeLocal || h.s.webhookOK || h.bridge.listCalls != 1 {
		t.Fatalf("mode %s ok %v lists %d", h.s.mode, h.s.webhookOK, h.bridge.listCalls)
	}
	h.advance(10 * time.Minute)
	if h.bridge.listCalls != 2 || h.bridge.statusCalls != 2 {
		t.Fatalf("retry + poll expected: lists %d status %d", h.bridge.listCalls, h.bridge.statusCalls)
	}
	h.bridge.listErr = nil
	h.advance(10 * time.Minute)
	if !h.s.webhookOK || len(h.bridge.created) != 1 {
		t.Fatalf("ok %v created %v", h.s.webhookOK, h.bridge.created)
	}
	status := h.bridge.statusCalls
	h.advance(10 * time.Minute)
	if h.bridge.statusCalls != status {
		t.Fatal("polling must stop once the webhook is registered")
	}
}

// A refresh that returns unusable credentials must not leave a nil bridge
// client in local mode (that panicked and crash-looped the process).
func TestUnusableRefreshLeavesLocalWithoutPanic(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	broken := testRecord()
	broken.LocalID = nil // the cloud stopped reporting local credentials
	h.refreshRec = &broken
	h.bridge.listErr = loqed.ErrUnauthorized // keys rotated
	h.start()
	if h.s.mode == model.ModeLocal || h.s.bridge != nil {
		t.Fatalf("mode %s bridge %v", h.s.mode, h.s.bridge)
	}
	h.run(30 * time.Second) // ticks must not touch the nil bridge
}

// TCP up but HTTP hung: probes succeed, requests time out. The HTTP failure
// count must not be reset by the probe.
func TestHungHTTPFailsOverDespiteTCPProbe(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.bridge.status.BoltState = loqed.BoltUnknown // causes /status every 10 min
	h.start()
	h.bridge.statusErr = loqed.ErrNoResponse
	for range 3 {
		h.advance(10 * time.Minute)
	}
	stale := false
	for _, st := range h.pub.states {
		stale = stale || st.StateStale
	}
	if h.s.mode != model.ModeCloud || !stale {
		t.Fatalf("mode %s; failed /status must have marked the state stale: %v", h.s.mode, stale)
	}
}

func TestBridgeEventAppliedWhenLocalEntryFails(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.toCloud()
	h.bridge.statusErr = loqed.ErrNoResponse
	h.send(reached("STATE_CHANGED_LATCH", model.Ptr(2)))
	if h.s.mode != model.ModeCloud || h.lock() != "UNLOCKED" {
		t.Fatalf("mode %s lock %s", h.s.mode, h.lock())
	}
}

func TestBridgeAddress(t *testing.T) {
	if BridgeAddress("192.0.2.10") != "192.0.2.10:80" || BridgeAddress("127.0.0.1:8080") != "127.0.0.1:8080" {
		t.Fatal("BridgeAddress")
	}
}
```

- [ ] **Step 3: Run to verify failure**

Run: `go test ./internal/gateway/ -run 'Local|Webhook|Bridge|Key|Online|Status|Unknown|Motor|Hung|Unusable|Pinned'`
Expected: FAIL — build errors such as `undefined: Supervisor` and `undefined: Deps`.

- [ ] **Step 4: Implement `supervisor.go`**

`internal/gateway/supervisor.go`:

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

// CloudSource is the supervisor's view of the CloudHub.
type CloudSource interface {
	Locks(ctx context.Context, p Priority, notBefore time.Time) (LockList, error)
	Command(ctx context.Context, lockID string, s loqed.BoltState) error
}

type Publisher interface {
	PublishState(lockID string, s model.State) error
	PublishEvent(lockID string, e model.Event) error
	PublishAvailability(lockID string, online bool) error
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
type CloudEventMsg struct{ Event cloud.WebhookEvent }
type CommandMsg struct {
	Command model.Command
	At      time.Time // MQTT arrival time; the command expires At+CommandMaxAge
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
	Now           func() time.Time
	Log           *slog.Logger
}

type Timing struct {
	Liveness         time.Duration // bridge and cloud TCP probes
	Reconcile        time.Duration // max interval between /status (local) or reconcile polls (cloud push)
	OfflineRetry     time.Duration
	UnknownRecheck   time.Duration
	WebhookConfirm   time.Duration // /status if no matching webhook arrives
	WebhookRetry     time.Duration // retry bridge webhook registration
	CloudConfirm     time.Duration
	CloudPoll        time.Duration // how often cloud mode asks the budget for a poll
	CloudPollSpacing time.Duration // budget spacing of background polls (12h / cloud_budget)
	StaleGrace       time.Duration
	CommandMaxAge    time.Duration
	EnrichWindow     time.Duration
	RequestTimeout   time.Duration
	FailureThreshold int
}

func DefaultTiming(liveness, reconcile, pollSpacing time.Duration) Timing {
	return Timing{
		Liveness: liveness, Reconcile: reconcile,
		OfflineRetry: 5 * time.Minute, UnknownRecheck: 10 * time.Minute,
		WebhookConfirm: 10 * time.Second, WebhookRetry: 10 * time.Minute,
		CloudConfirm: 5 * time.Second, CloudPoll: time.Minute, CloudPollSpacing: pollSpacing,
		StaleGrace:    10 * time.Minute,
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
	keyID     *int
	at        time.Time
}

const warnRepeatWindow = 10 * time.Minute

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

	// Command / movement confirmation.
	confirmTarget     loqed.BoltState // BoltUnknown: any reached state confirms
	confirmAt         time.Time       // local: /status at this time
	confirmViaCloud   bool            // if that /status fails, ask the cloud
	cloudConfirmAt    time.Time       // cloud confirmation poll at this time
	cloudConfirmSince time.Time       // only data fetched after this counts

	lastFreshAt      time.Time // last fresh bolt data (status, poll, event)
	lastEventAt      time.Time // last applied lock event
	lastPollAt       time.Time // last successful cloud poll
	lastBridgeEvent  *recentEvent
	lastCloudEventAt time.Time

	warned map[string]*warnState
}

func NewSupervisor(rec store.LockRecord, setting config.LockSetting, d Deps, t Timing) *Supervisor {
	return &Supervisor{
		id: rec.ID, d: d, t: t, setting: setting, in: make(chan any, 64),
		log:    d.Log.With("lock", rec.Name, "lock_id", rec.ID),
		rec:    ApplySetting(rec, setting),
		mode:   model.ModeOffline,
		state:  model.State{BoltState: loqed.BoltUnknown, Mode: model.ModeOffline},
		warned: map[string]*warnState{},
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
}

func (s *Supervisor) handle(ctx context.Context, m any) {
	switch m := m.(type) {
	case BridgeEventMsg:
		s.onBridgeEvent(ctx, m.Event)
	case CloudEventMsg:
		s.onCloudEvent(ctx, m.Event)
	case CommandMsg:
		s.onCommand(ctx, m)
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
	s.mode, s.state.Mode = m, m
	s.probeFailures, s.httpFailures, s.cloudProbeFailures, s.cloudAPIFailures = 0, 0, 0, 0
}

func (s *Supervisor) setRecord(rec store.LockRecord) {
	s.mu.Lock()
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
func (s *Supervisor) recordEvent(now time.Time, eventType string, rawKey *int, cloudKeyName string, t model.Transition, fromBridge bool) {
	s.state.Apply(t)
	s.state.StateStale = false
	s.lastFreshAt, s.lastEventAt = now, now
	key := model.NormalizeKeyID(rawKey)
	name := s.keyName(key, cloudKeyName)
	at := now.UTC().Truncate(time.Second)
	s.state.LastEvent, s.state.LastKeyID, s.state.LastKeyName, s.state.LastEventAt = eventType, key, name, &at
	if fromBridge {
		s.lastBridgeEvent = &recentEvent{eventType: strings.ToUpper(eventType), keyID: key, at: now}
	}
	s.publish()
	ev := model.Event{EventType: t.Event, Reason: eventType, Source: model.Source(eventType), KeyLocalID: key, KeyName: name}
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
}

// scheduleCloudConfirm polls the cloud CloudConfirm after a cloud command,
// accepting only data fetched after the command.
func (s *Supervisor) scheduleCloudConfirm(now time.Time, target loqed.BoltState) {
	s.confirmTarget = target
	s.cloudConfirmAt = now.Add(s.t.CloudConfirm)
	s.cloudConfirmSince = now
}

// commandFailed reports a command failure to HA as a command_failed event.
func (s *Supervisor) commandFailed(c model.Command, class string, err error) {
	s.log.Error("lock command failed", "command", c, "error_class", class, "err", err)
	ev := model.Event{EventType: model.EventCommandFailed, Reason: string(c), Source: model.SourceGateway, Error: class}
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

// failClass maps an error onto a command_failed class.
func failClass(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		if errors.Is(err, loqed.ErrNoResponse) {
			return model.FailNoResponse
		}
		return model.FailExpired
	case errors.Is(err, loqed.ErrNoResponse), loqed.IsServerError(err):
		return model.FailNoResponse
	case errors.Is(err, loqed.ErrUnreachable), errors.Is(err, errNoBridge):
		return model.FailUnreachable
	case errors.Is(err, loqed.ErrUnauthorized):
		return model.FailUnauthorized
	case errors.Is(err, loqed.ErrRateLimited):
		return model.FailRateLimited
	default:
		return model.FailOther
	}
}
```

- [ ] **Step 5: Implement `local.go`**

`internal/gateway/local.go`:

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

// ensureBridge builds the bridge client if needed (no network I/O).
func (s *Supervisor) ensureBridge() bool {
	if s.bridge != nil {
		return true
	}
	if !s.canLocal() {
		return false
	}
	b, err := s.d.NewBridge(s.Record())
	if err != nil {
		s.warn("cannot create bridge client", "err", err)
		return false
	}
	s.bridge = b
	return true
}

// tryEnterLocal fetches status and, on success, switches to local mode and
// makes sure our webhook is registered.
func (s *Supervisor) tryEnterLocal(ctx context.Context) bool {
	if !s.ensureBridge() {
		return false
	}
	st, err := s.status(ctx)
	if err != nil {
		s.log.Debug("bridge not reachable", "err", err)
		return false
	}
	now := s.d.Now()
	s.setMode(model.ModeLocal)
	s.applyStatus(now, st)
	s.nextProbe = now.Add(s.t.Liveness)
	s.nextReconcile = now.Add(s.t.Reconcile)
	if !s.registerWebhook(ctx) {
		return true // registerWebhook left local mode
	}
	s.publish()
	return true
}

func (s *Supervisor) status(ctx context.Context) (*bridge.Status, error) {
	if s.bridge == nil {
		return nil, errNoBridge
	}
	c, cancel := s.reqCtx(ctx)
	defer cancel()
	return s.bridge.Status(c)
}

func (s *Supervisor) applyStatus(now time.Time, st *bridge.Status) {
	s.state.BoltState = st.BoltState
	s.state.Lock = model.LockStateFor(st.BoltState)
	s.state.BatteryPercentage = model.Ptr(int(st.BatteryPercentage))
	s.state.BatteryVoltage = model.Ptr(float64(st.BatteryVoltage))
	s.state.WifiStrength = model.Ptr(int(st.WifiStrength))
	s.state.BLEStrength = model.Ptr(int(st.BLEStrength))
	s.state.LockOnline = st.LockOnline == 1
	s.state.StateStale = false
	s.lastFreshAt = now
	if st.BoltState == loqed.BoltUnknown {
		s.lastUnknownCheck = now
	}
}

// registerWebhook ensures our bridge webhook. On failure it schedules a
// retry; an auth failure refreshes credentials first. It returns false if
// the lock had to leave local mode (no usable bridge client).
func (s *Supervisor) registerWebhook(ctx context.Context) bool {
	err := s.ensureWebhook(ctx)
	if errors.Is(err, loqed.ErrUnauthorized) {
		if !s.refreshAndRebuild(ctx, ReasonUnauthorized) {
			if s.bridge == nil {
				s.enterCloud(ctx)
				return false
			}
		} else {
			err = s.ensureWebhook(ctx)
		}
	}
	s.webhookOK = err == nil
	if err != nil {
		s.nextWebhookRetry = s.d.Now().Add(s.t.WebhookRetry)
		s.warn("could not register the webhook on the bridge; polling /status every 10 min until it works", "err", err)
	}
	return true
}

// ensureWebhook registers <private>/webhook/<id> and removes our stale
// registrations (same path, different host or port). Other webhooks stay.
func (s *Supervisor) ensureWebhook(ctx context.Context) error {
	if s.bridge == nil {
		return errNoBridge
	}
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
				s.warn("could not delete a stale webhook", "webhook_id", int(h.ID), "err", err)
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
	if s.bridge == nil && !s.ensureBridge() {
		s.enterCloud(ctx) // credentials became unusable
		return
	}
	steps := []func() bool{
		func() bool { // a command's or movement's webhook never arrived
			if s.confirmAt.IsZero() || now.Before(s.confirmAt) {
				return true
			}
			s.confirmAt = time.Time{}
			viaCloud := s.confirmViaCloud
			s.confirmViaCloud = false
			if !s.reconcile(ctx) && viaCloud {
				s.scheduleCloudConfirm(now, s.confirmTarget)
			}
			return s.mode == model.ModeLocal
		},
		func() bool { // webhook registration pending
			if s.webhookOK || now.Before(s.nextWebhookRetry) {
				return true
			}
			s.nextWebhookRetry = now.Add(s.t.WebhookRetry)
			if !s.registerWebhook(ctx) {
				return false
			}
			s.reconcile(ctx) // no webhooks yet: poll instead
			return s.mode == model.ModeLocal
		},
		func() bool { // TCP liveness
			if now.Before(s.nextProbe) {
				return true
			}
			s.nextProbe = now.Add(s.t.Liveness)
			s.probeLocal(ctx)
			return s.mode == model.ModeLocal
		},
		func() bool { // unknown bolt: recheck at most every 10 min
			if s.state.BoltState != loqed.BoltUnknown || now.Sub(s.lastUnknownCheck) < s.t.UnknownRecheck {
				return true
			}
			s.reconcile(ctx)
			return s.mode == model.ModeLocal
		},
		func() bool { // periodic reconcile (also retries webhook registration)
			if now.Before(s.nextReconcile) {
				return true
			}
			if !s.webhookOK && !s.registerWebhook(ctx) {
				return false
			}
			s.reconcile(ctx)
			return s.mode == model.ModeLocal
		},
	}
	for _, step := range steps {
		if !step() {
			return
		}
	}
}

// reconcile fetches /status; it reports whether that worked.
func (s *Supervisor) reconcile(ctx context.Context) bool {
	now := s.d.Now()
	s.nextReconcile = now.Add(s.t.Reconcile)
	if s.state.BoltState == loqed.BoltUnknown {
		s.lastUnknownCheck = now
	}
	st, err := s.status(ctx)
	if err != nil {
		s.markStale()
		s.httpFailure(ctx, err)
		return false
	}
	s.httpFailures = 0
	s.applyStatus(now, st)
	s.publish()
	return true
}

// probeLocal is the TCP liveness check. Success only resets the probe
// counter: a bridge that accepts TCP but hangs on HTTP must still fail over.
func (s *Supervisor) probeLocal(ctx context.Context) {
	c, cancel := s.reqCtx(ctx)
	defer cancel()
	if err := s.d.Probe(c, BridgeAddress(s.Record().BridgeIP)); err != nil {
		s.probeFailures++
		s.log.Debug("bridge probe failed", "failures", s.probeFailures, "err", err)
		if s.probeFailures >= s.t.FailureThreshold {
			s.localUnreachable(ctx, err)
		}
		return
	}
	s.probeFailures = 0
}

// httpFailure counts a failed bridge HTTP request.
func (s *Supervisor) httpFailure(ctx context.Context, err error) {
	if errors.Is(err, loqed.ErrUnauthorized) {
		if !s.refreshAndRebuild(ctx, ReasonUnauthorized) && s.bridge == nil {
			s.enterCloud(ctx)
		}
		return
	}
	s.httpFailures++
	s.log.Debug("bridge request failed", "failures", s.httpFailures, "err", err)
	if s.httpFailures >= s.t.FailureThreshold {
		s.localUnreachable(ctx, err)
	}
}

// localUnreachable tries a credential refresh (the IP may have changed,
// unless pinned in lock_settings) and otherwise falls back to cloud mode.
func (s *Supervisor) localUnreachable(ctx context.Context, err error) {
	s.warn("bridge unreachable", "err", err)
	oldIP := s.Record().BridgeIP
	if s.refreshAndRebuild(ctx, ReasonUnreachable) && s.Record().BridgeIP != oldIP {
		s.log.Info("bridge IP changed", "old", oldIP, "new", s.Record().BridgeIP)
		if s.tryEnterLocal(ctx) {
			return
		}
	}
	s.enterCloud(ctx)
}

// refreshAndRebuild reloads credentials from the cloud. It is skipped when
// lock_settings pins what the refresh would change. On success the bridge
// client is rebuilt; if that fails, s.bridge stays nil.
func (s *Supervisor) refreshAndRebuild(ctx context.Context, reason Reason) bool {
	switch {
	case reason == ReasonUnauthorized && KeysPinned(s.setting):
		s.warn("the bridge rejected the keys pinned in lock_settings; fix bridge_key/key_secret/local_id")
		return false
	case reason == ReasonUnreachable && IPPinned(s.setting):
		return false
	}
	rec, err := s.d.Refresh(ctx, s.id, reason)
	if err != nil {
		if !errors.Is(err, ErrRefreshThrottled) {
			s.warn("credential refresh failed", "reason", reason, "err", err)
		}
		return false
	}
	s.setRecord(rec)
	return s.ensureBridge()
}

func (s *Supervisor) onBridgeEvent(ctx context.Context, ev bridge.Event) {
	// The event is signed with the bridge key, so it is authentic even if
	// entering local mode fails; apply it either way.
	if s.mode != model.ModeLocal {
		s.tryEnterLocal(ctx)
	}
	now := s.d.Now()
	if s.mode == model.ModeLocal {
		s.probeFailures, s.httpFailures = 0, 0
		s.nextProbe = now.Add(s.t.Liveness) // a webhook proves the bridge is alive
	}
	switch e := ev.(type) {
	case bridge.StateReachedEvent:
		s.state.LockOnline = true
		s.onReached(now, e.BoltState, e.Jammed)
		s.recordEvent(now, e.EventType, e.KeyLocalID, "", model.FromStateReached(e.EventType), true)
	case bridge.GoToStateEvent:
		s.recordEvent(now, e.EventType, e.KeyLocalID, "", model.FromGoTo(e.GoToState, s.state.Lock), true)
		if s.confirmAt.IsZero() {
			s.awaitConfirm(now, e.GoToState) // STATE_CHANGED may be lost
		}
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

- [ ] **Step 6: Add the placeholders**

These keep the package compiling; Tasks 10–12 replace each file entirely.

`internal/gateway/cloudmode.go`:

```go
package gateway

import (
	"context"
	"time"

	"github.com/t3hk0d3/go-loqed/internal/model"
)

// Placeholder until Task 10 replaces this file with cloud and offline modes.

func (s *Supervisor) enterCloud(context.Context) {
	s.setMode(model.ModeCloud)
	s.publish()
}

func (s *Supervisor) tickCloud(context.Context, time.Time)   {}
func (s *Supervisor) tickOffline(context.Context, time.Time) {}

func (s *Supervisor) pollCloud(context.Context, Priority, time.Time) bool { return false }
```

`internal/gateway/commands.go`:

```go
package gateway

import "context"

// Placeholder until Task 11 replaces this file with command handling.

func (s *Supervisor) onCommand(_ context.Context, m CommandMsg) {
	s.log.Warn("lock commands are not implemented yet", "command", m.Command)
}
```

`internal/gateway/cloudevents.go`:

```go
package gateway

import (
	"context"

	"github.com/t3hk0d3/go-loqed/cloud"
)

// Placeholder until Task 12 replaces this file with cloud webhook handling.

func (s *Supervisor) onCloudEvent(context.Context, cloud.WebhookEvent) {}
```

- [ ] **Step 7: Run tests**

Run: `gofmt -l internal/gateway && go vet ./internal/gateway/ && go test ./internal/gateway/ -v -race`
Expected: PASS (38 tests). Some harness helpers (`command`, `failedCommands`, the cloud fakes) are only used from Task 10 on.

- [ ] **Step 8: Commit**

```bash
git add internal/gateway
git commit -m "gateway: add the per-lock supervisor and local mode

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 10: Cloud fallback and offline mode

**Files:**
- Replace: `internal/gateway/cloudmode.go`
- Create: `internal/gateway/failover_test.go`

**Interfaces:**
- Consumes: everything from Task 9; `ErrDeferred`, `ErrBudgetExhausted`, `ErrCloudBlocked`, `Deps.ProbeCloud`.
- Produces: `enterCloud(ctx)`, `enterOffline()`, `bridgeProbeSchedule(now)`, `tickCloud`, `tickOffline`, `pushActive(now) bool`, `nextPollTime(now) time.Time`, `checkFreshness(now)`, `probeBridge(ctx) bool`, `probeCloud(ctx) error`, `pollCloud(ctx, Priority, notBefore time.Time) bool`, `applyCloudLock(now, cloud.Lock, fetchedAt time.Time)`.

Behavior: entering cloud marks the state stale until fresh data arrives. Offline is decided by an unbudgeted TCP probe of the cloud host (3 failures) or 3 failed API calls; offline retries every 5 min use only TCP probes. `state_stale` turns on when no fresh data arrived within the poll spacing (or `reconcile_interval` with working cloud webhooks) plus 10 min. Poll data fetched before the last applied event never overwrites state; a missing `online` field keeps the previous value.

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
	if h.lock() != "LOCKED" || h.state().Mode != model.ModeCloud || !h.available() || h.state().StateStale {
		t.Fatalf("lock %s state %+v", h.lock(), h.state())
	}
}

func TestIPChangeReconnectsLocally(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	moved := testRecord()
	moved.BridgeIP = "192.0.2.77"
	h.refreshRec = &moved
	h.probeErr = loqed.ErrUnreachable
	for range 3 {
		h.advance(60 * time.Second)
	}
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

// Offline detection uses an unbudgeted TCP probe of the cloud host, so it
// takes minutes, not hours of deferred polls.
func TestCloudProbeFailuresGoOfflineAndRecoverWithinFiveMinutes(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.toCloud()
	h.cloudProbeErr = loqed.ErrUnreachable
	for range 3 {
		h.advance(time.Minute)
	}
	if h.s.mode != model.ModeOffline || h.available() || h.state().Mode != model.ModeOffline {
		t.Fatalf("mode %s", h.s.mode)
	}
	h.cloudProbeErr = nil
	h.advance(4 * time.Minute)
	if h.s.mode != model.ModeOffline {
		t.Fatal("must wait 5 minutes before retrying")
	}
	h.advance(time.Minute)
	if h.s.mode != model.ModeCloud {
		t.Fatalf("mode %s", h.s.mode)
	}
}

func TestCloudAPIFailuresGoOffline(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.toCloud()
	h.cloud.err = loqed.ErrNoResponse
	for range 3 {
		h.advance(time.Minute)
	}
	if h.s.mode != model.ModeOffline {
		t.Fatalf("mode %s", h.s.mode)
	}
}

func TestOfflineRetriesForeverWithoutSpendingBudget(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.toCloud()
	h.cloudProbeErr = loqed.ErrUnreachable
	for range 3 {
		h.advance(time.Minute)
	}
	before, calls := len(h.probes), len(h.cloud.calls)
	for range 24 { // two hours
		h.advance(5 * time.Minute)
	}
	if h.s.mode != model.ModeOffline || len(h.probes)-before != 24 || len(h.cloud.calls) != calls {
		t.Fatalf("mode %s probes %d cloud calls %d", h.s.mode, len(h.probes)-before, len(h.cloud.calls)-calls)
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
	h.cloud.err = nil
	h.cloud.locks[0].BoltState = loqed.BoltDayLock
	h.advance(time.Minute)
	if h.state().StateStale || h.lock() != "UNLOCKED" {
		t.Fatalf("fresh poll must clear stale: %+v", h.state())
	}
}

func TestEnteringCloudIsStaleUntilFreshData(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.cloud.err = ErrDeferred
	h.toCloud()
	if !h.state().StateStale || h.lock() != "UNLOCKED" {
		t.Fatalf("last local state must be marked stale in cloud mode: %+v", h.state())
	}
}

func TestDeferredPollsEventuallyMarkStale(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.toCloud() // fresh poll
	h.cloud.err = ErrDeferred
	h.advance(80 * time.Minute)
	if h.state().StateStale {
		t.Fatal("within spacing + grace")
	}
	h.advance(5 * time.Minute)
	if !h.state().StateStale {
		t.Fatal("no fresh data for longer than spacing + grace must be stale")
	}
}

func TestPollOlderThanEventIsIgnored(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.toCloud()
	h.bridge.statusErr = loqed.ErrNoResponse    // stay in cloud mode
	h.send(reached("STATE_CHANGED_LATCH", nil)) // event at now
	h.cloud.fetchedAt = h.now.Add(-time.Second) // poll data from before it
	h.cloud.locks[0].BoltState = loqed.BoltNightLock
	h.s.pollCloud(t.Context(), PriorityConfirm, time.Time{})
	if h.lock() != "UNLOCKED" {
		t.Fatalf("older poll overwrote newer event: %s", h.lock())
	}
}

func TestMissingOnlineKeepsPreviousValue(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.toCloud()
	h.cloud.locks[0].Online = model.Ptr(false)
	h.s.pollCloud(t.Context(), PriorityConfirm, time.Time{})
	h.cloud.locks[0].Online = nil
	h.s.pollCloud(t.Context(), PriorityConfirm, time.Time{})
	if h.state().LockOnline {
		t.Fatal("absent online must keep the previous value")
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

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/gateway/ -run 'Failures|IPChange|PinnedBridge|CloudMode|CloudProbe|CloudAPI|Offline|Budget|Entering|Deferred|PollOlder|MissingOnline|CloudOnly'`
Expected: FAIL — e.g. `TestThreeFailuresSwitchToCloud: cloud calls []` (the placeholder does not poll).

- [ ] **Step 3: Replace `cloudmode.go`**

`internal/gateway/cloudmode.go`:

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

// enterCloud switches to cloud mode. State is stale until fresh cloud data
// (a poll or a cloud webhook) arrives.
func (s *Supervisor) enterCloud(ctx context.Context) {
	now := s.d.Now()
	s.setMode(model.ModeCloud)
	s.bridgeProbeSchedule(now)
	s.nextCloudProbe = now.Add(s.t.Liveness)
	s.nextCloudPoll = now.Add(s.t.CloudPoll)
	s.confirmAt, s.confirmViaCloud = time.Time{}, false
	s.state.StateStale = true
	s.publish()
	s.pollCloud(ctx, PriorityBackground, time.Time{})
}

func (s *Supervisor) bridgeProbeSchedule(now time.Time) {
	s.nextProbe = now.Add(s.t.Liveness)
}

func (s *Supervisor) enterOffline() {
	s.setMode(model.ModeOffline)
	s.nextOfflineRetry = s.d.Now().Add(s.t.OfflineRetry)
	s.publish()
}

func (s *Supervisor) tickCloud(ctx context.Context, now time.Time) {
	if s.canLocal() && !now.Before(s.nextProbe) {
		s.nextProbe = now.Add(s.t.Liveness)
		if s.probeBridge(ctx) && s.tryEnterLocal(ctx) {
			return
		}
	}
	if !now.Before(s.nextCloudProbe) {
		s.nextCloudProbe = now.Add(s.t.Liveness)
		if err := s.probeCloud(ctx); err != nil {
			s.cloudProbeFailures++
			s.log.Debug("cloud probe failed", "failures", s.cloudProbeFailures, "err", err)
			if s.cloudProbeFailures >= s.t.FailureThreshold {
				s.warn("LOQED cloud unreachable", "err", err)
				s.enterOffline()
				return
			}
		} else {
			s.cloudProbeFailures = 0
		}
	}
	if !now.Before(s.nextCloudPoll) {
		s.nextCloudPoll = s.nextPollTime(now)
		s.pollCloud(ctx, PriorityBackground, time.Time{})
		if s.mode != model.ModeCloud {
			return
		}
	}
	s.checkFreshness(now)
}

// pushActive: cloud webhooks are configured and have been seen recently.
func (s *Supervisor) pushActive(now time.Time) bool {
	return s.d.CloudWebhooks && !s.lastCloudEventAt.IsZero() && now.Sub(s.lastCloudEventAt) < s.t.Reconcile
}

// nextPollTime: with working cloud webhooks only one reconcile poll per
// Reconcile, counted from the last successful poll (events never postpone
// it); otherwise ask every CloudPoll and let the budget space the calls.
func (s *Supervisor) nextPollTime(now time.Time) time.Time {
	next := now.Add(s.t.CloudPoll)
	if s.pushActive(now) && !s.lastPollAt.IsZero() {
		if r := s.lastPollAt.Add(s.t.Reconcile); r.After(next) {
			return r
		}
	}
	return next
}

// checkFreshness marks the state stale when no fresh data arrived within
// the expected interval plus StaleGrace.
func (s *Supervisor) checkFreshness(now time.Time) {
	expected := s.t.CloudPollSpacing
	if s.pushActive(now) {
		expected = s.t.Reconcile
	}
	if now.Sub(s.lastFreshAt) > expected+s.t.StaleGrace {
		s.markStale()
	}
}

func (s *Supervisor) tickOffline(ctx context.Context, now time.Time) {
	if now.Before(s.nextOfflineRetry) {
		return
	}
	s.nextOfflineRetry = now.Add(s.t.OfflineRetry)
	if s.canLocal() && s.probeBridge(ctx) && s.tryEnterLocal(ctx) {
		return
	}
	if err := s.probeCloud(ctx); err != nil {
		s.log.Debug("cloud still unreachable", "err", err)
		return
	}
	s.enterCloud(ctx)
}

func (s *Supervisor) probeBridge(ctx context.Context) bool {
	c, cancel := s.reqCtx(ctx)
	defer cancel()
	return s.d.Probe(c, BridgeAddress(s.Record().BridgeIP)) == nil
}

func (s *Supervisor) probeCloud(ctx context.Context) error {
	if s.d.ProbeCloud == nil {
		return nil
	}
	c, cancel := s.reqCtx(ctx)
	defer cancel()
	return s.d.ProbeCloud(c)
}

// pollCloud reads the lock from the cloud. It returns true on fresh data.
func (s *Supervisor) pollCloud(ctx context.Context, p Priority, notBefore time.Time) bool {
	list, err := s.d.Cloud.Locks(ctx, p, notBefore)
	switch {
	case errors.Is(err, ErrDeferred):
		return false // freshness tracking marks the state stale if this lasts
	case errors.Is(err, ErrBudgetExhausted), errors.Is(err, ErrCloudBlocked), errors.Is(err, loqed.ErrRateLimited):
		s.markStale()
		return false
	case err != nil:
		s.cloudAPIFailures++
		s.warn("cloud request failed", "failures", s.cloudAPIFailures, "err", err)
		if s.mode == model.ModeCloud && s.cloudAPIFailures >= s.t.FailureThreshold {
			s.enterOffline()
		}
		return false
	}
	now := s.d.Now()
	s.cloudAPIFailures = 0
	s.lastPollAt = now
	for _, l := range list.Locks {
		if l.ID != s.id {
			continue
		}
		if s.mode == model.ModeOffline {
			s.setMode(model.ModeCloud)
			s.bridgeProbeSchedule(now)
			s.nextCloudProbe, s.nextCloudPoll = now.Add(s.t.Liveness), now.Add(s.t.CloudPoll)
		}
		s.applyCloudLock(now, l, list.FetchedAt)
		s.publish()
		return true
	}
	s.warn("lock is missing from the cloud lock list")
	return false
}

// applyCloudLock applies polled data. Bolt data older than the last applied
// event is ignored (a poll never overwrites newer webhook state). A missing
// online field keeps the previous value.
func (s *Supervisor) applyCloudLock(now time.Time, l cloud.Lock, fetchedAt time.Time) {
	if !fetchedAt.Before(s.lastEventAt) {
		s.state.BoltState = l.BoltState
		s.state.Lock = model.LockStateFor(l.BoltState)
		s.state.StateStale = false
		s.lastFreshAt = now
		if s.confirmTarget == loqed.BoltUnknown || s.confirmTarget == l.BoltState {
			s.cloudConfirmAt = time.Time{}
		}
	}
	s.state.BatteryPercentage = model.Ptr(l.BatteryPercentage)
	if l.Online != nil {
		s.state.LockOnline = *l.Online
	}
}
```

- [ ] **Step 4: Run tests**

Run: `gofmt -l internal/gateway && go test ./internal/gateway/ -v -race`
Expected: PASS (51 tests).

- [ ] **Step 5: Commit**

```bash
git add internal/gateway
git commit -m "gateway: add cloud fallback, offline mode and freshness tracking

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 11: Commands

**Files:**
- Replace: `internal/gateway/commands.go`
- Create: `internal/gateway/commands_test.go`

**Interfaces:**
- Consumes: `CommandMsg`, `model.Command.Target/Moving`, `model.Fail*`, `httpFailure`, `refreshAndRebuild`, `ensureBridge`, `awaitConfirm`, `scheduleCloudConfirm`, `commandFailed`, `failClass`, `errNoBridge`.
- Produces: `onCommand(ctx, CommandMsg)`, `sendViaBridge(ctx, cctx context.Context, model.Command)`, `bridgeCommand(ctx, model.Command) error`, `sendViaCloud(cctx context.Context, model.Command)`, `actionFor(model.Command) bridge.Action`.

Rules (spec §5.5, safety-critical):
- The command context times out at `At + CommandMaxAge` (computed on the supervisor clock, passed as remaining time); nothing is sent after that.
- Bridge `ErrUnreachable` / no usable client → send via the cloud now, then count the failure.
- Bridge `ErrUnauthorized` → send via the cloud now (the bridge rejected the signature, so nothing happened), then refresh credentials.
- Anything else (`ErrNoResponse`, 5xx, deadline after sending…) → **never resend**; publish `command_failed`, check `/status` on the next tick and, if that fails too, poll the cloud.
- A cloud command always schedules a fresh confirmation poll (also when its own response timed out).
- Failures publish a `command_failed` event with an error class.

- [ ] **Step 1: Write failing tests**

`internal/gateway/commands_test.go`:

```go
package gateway

import (
	"slices"
	"testing"
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/bridge"
	"github.com/t3hk0d3/go-loqed/internal/config"
	"github.com/t3hk0d3/go-loqed/internal/model"
)

func TestLocalCommandUsesBridge(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.command(model.CommandLock)
	if len(h.bridge.commands) != 1 || h.bridge.commands[0] != bridge.ActionLock || len(h.cloud.commands) != 0 {
		t.Fatalf("bridge %v cloud %v", h.bridge.commands, h.cloud.commands)
	}
	h.command(model.CommandOpen)
	h.command(model.CommandUnlock)
	if h.bridge.commands[1] != bridge.ActionOpen || h.bridge.commands[2] != bridge.ActionUnlock {
		t.Fatalf("bridge %v", h.bridge.commands)
	}
}

func TestStaleCommandIsDroppedAndReported(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.send(CommandMsg{Command: model.CommandOpen, At: h.now.Add(-11 * time.Second)})
	if len(h.bridge.commands) != 0 || len(h.cloud.commands) != 0 {
		t.Fatal("a stale OPEN must never fire")
	}
	if got := h.failedCommands(); !slices.Equal(got, []string{model.FailExpired}) {
		t.Fatalf("command_failed %v", got)
	}
	ev := h.pub.events[0]
	if ev.Reason != "OPEN" || ev.Source != model.SourceGateway {
		t.Fatalf("event %+v", ev)
	}
}

// The bridge provably did not get the command: send it via the cloud, in
// the same request, within the command's deadline.
func TestUndeliveredCommandFallsBackToCloud(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.bridge.commandErrs = []error{loqed.ErrUnreachable}
	h.command(model.CommandLock)
	if len(h.cloud.commands) != 1 || h.cloud.commands[0] != loqed.BoltNightLock || h.lock() != "LOCKING" {
		t.Fatalf("cloud %v lock %s", h.cloud.commands, h.lock())
	}
	if d := h.cloud.cmdBudgets[0]; d > 10*time.Second || d < 9*time.Second {
		t.Fatalf("cloud command must run within the 10s command deadline: %v", d)
	}
	if h.s.httpFailures != 1 || len(h.refreshes) != 0 {
		t.Fatalf("failure counted after the command, no refresh yet: %d %v", h.s.httpFailures, h.refreshes)
	}
}

// The bridge may have acted (timeout after sending): never resend via the
// cloud (that would unlatch the door twice); verify and report instead.
func TestCommandWithoutResponseIsNeverResent(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.bridge.commandErrs = []error{loqed.ErrNoResponse}
	h.command(model.CommandOpen)
	if len(h.cloud.commands) != 0 || len(h.bridge.commands) != 1 {
		t.Fatalf("resent: bridge %v cloud %v", h.bridge.commands, h.cloud.commands)
	}
	if got := h.failedCommands(); !slices.Equal(got, []string{model.FailNoResponse}) {
		t.Fatalf("command_failed %v", got)
	}
	h.bridge.status.BoltState = loqed.BoltOpen
	h.advance(time.Second)
	if h.bridge.statusCalls != 2 || h.lock() != "OPEN" {
		t.Fatalf("immediate status expected: %d %s", h.bridge.statusCalls, h.lock())
	}
}

func TestCommandWithoutResponseFallsBackToCloudConfirm(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.bridge.commandErrs = []error{loqed.ErrNoResponse}
	h.command(model.CommandLock)
	h.bridge.statusErr = loqed.ErrNoResponse
	h.advance(time.Second) // status fails
	calls := len(h.cloud.calls)
	h.run(6 * time.Second)
	if len(h.cloud.calls) != calls+1 || h.cloud.calls[calls] != PriorityConfirm || len(h.cloud.commands) != 0 {
		t.Fatalf("calls %v commands %v", h.cloud.calls, h.cloud.commands)
	}
}

func TestCommandAuthErrorUsesCloudThenRefreshes(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.bridge.commandErrs = []error{loqed.ErrUnauthorized}
	h.command(model.CommandUnlock)
	if len(h.bridge.commands) != 1 || len(h.cloud.commands) != 1 || len(h.refreshes) != 1 || h.refreshes[0] != ReasonUnauthorized {
		t.Fatalf("bridge %v cloud %v refreshes %v", h.bridge.commands, h.cloud.commands, h.refreshes)
	}
}

// A command that cannot reach the bridge until its deadline is never sent
// late through the cloud.
func TestCommandPastDeadlineIsNotSentViaCloud(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.bridge.commandErrs = []error{loqed.ErrUnreachable}
	h.send(CommandMsg{Command: model.CommandOpen, At: h.now.Add(-9 * time.Second)})
	if len(h.cloud.commands) != 1 || h.cloud.cmdBudgets[0] > time.Second {
		t.Fatalf("the cloud call must carry only the remaining 1s: %v", h.cloud.cmdBudgets)
	}
}

func TestMissedWebhookTriggersStatusAfterTenSeconds(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.command(model.CommandLock)
	h.run(9 * time.Second)
	if h.bridge.statusCalls != 1 {
		t.Fatal("too early")
	}
	h.run(time.Second)
	if h.bridge.statusCalls != 2 {
		t.Fatalf("status calls %d", h.bridge.statusCalls)
	}
}

func TestWebhookConfirmationCancelsStatus(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.command(model.CommandLock)
	h.send(reached("STATE_CHANGED_NIGHT_LOCK", nil))
	h.run(15 * time.Second)
	if h.bridge.statusCalls != 1 {
		t.Fatalf("status calls %d", h.bridge.statusCalls)
	}
}

func TestOfflineRejectsCommands(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.toCloud()
	h.cloudProbeErr = loqed.ErrUnreachable
	for range 3 {
		h.advance(time.Minute)
	}
	h.command(model.CommandOpen)
	if len(h.cloud.commands) != 0 || len(h.bridge.commands) != 0 {
		t.Fatal("offline must reject commands")
	}
	if got := h.failedCommands(); !slices.Equal(got, []string{model.FailOffline}) {
		t.Fatalf("command_failed %v", got)
	}
}

func TestCloudModeCommandConfirmPollIgnoresOlderData(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.toCloud()
	calls := len(h.cloud.calls)
	at := h.now
	h.command(model.CommandUnlock)
	if len(h.cloud.commands) != 1 || h.cloud.commands[0] != loqed.BoltDayLock || h.lock() != "UNLOCKING" {
		t.Fatalf("cloud %v lock %s", h.cloud.commands, h.lock())
	}
	h.run(5 * time.Second)
	if len(h.cloud.calls) != calls+1 || h.cloud.calls[calls] != PriorityConfirm || !h.cloud.notBefore[calls].Equal(at) {
		t.Fatalf("calls %v notBefore %v", h.cloud.calls, h.cloud.notBefore)
	}
}

// After a local→cloud fallback the confirmation must come from the cloud;
// the bridge is down, so a /status confirm would leave UNLOCKING for an hour.
func TestFallbackCommandIsConfirmedViaCloud(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.bridge.commandErrs = []error{loqed.ErrUnreachable}
	h.command(model.CommandUnlock)
	h.cloud.locks[0].BoltState = loqed.BoltDayLock
	h.run(5 * time.Second)
	if h.lock() != "UNLOCKED" || h.cloud.calls[len(h.cloud.calls)-1] != PriorityConfirm {
		t.Fatalf("lock %s calls %v", h.lock(), h.cloud.calls)
	}
}

// A cloud command whose response timed out may have run: confirm anyway.
func TestCloudCommandTimeoutStillConfirms(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.toCloud()
	h.cloud.commandErr = loqed.ErrNoResponse
	calls := len(h.cloud.calls)
	h.command(model.CommandLock)
	h.run(5 * time.Second)
	if len(h.cloud.calls) != calls+1 || h.cloud.calls[calls] != PriorityConfirm {
		t.Fatalf("calls %v", h.cloud.calls)
	}
	if got := h.failedCommands(); !slices.Equal(got, []string{model.FailNoResponse}) {
		t.Fatalf("command_failed %v", got)
	}
}

// A refresh that returns unusable credentials must not leave a nil bridge
// client in local mode (that panicked and crash-looped the process).
func TestCommandWithUnusableRefreshUsesCloudWithoutPanic(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	broken := testRecord()
	broken.LocalID = nil // the cloud stopped reporting local credentials
	h.refreshRec = &broken
	h.bridge.commandErrs = []error{loqed.ErrUnauthorized}
	h.command(model.CommandLock)
	if h.s.mode != model.ModeCloud || h.s.bridge != nil {
		t.Fatalf("mode %s bridge %v", h.s.mode, h.s.bridge)
	}
	h.run(30 * time.Second) // ticks must not touch the nil bridge
	if len(h.cloud.commands) != 1 {
		t.Fatalf("the command must still go out via the cloud: %v", h.cloud.commands)
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/gateway/ -run 'Command|Undelivered|Stale|Missed|WebhookConfirmation|OfflineRejects|Fallback'`
Expected: FAIL — commands are only logged by the placeholder (`bridge [] cloud []`).

- [ ] **Step 3: Replace `commands.go`**

`internal/gateway/commands.go`:

```go
package gateway

import (
	"context"
	"errors"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/bridge"
	"github.com/t3hk0d3/go-loqed/internal/model"
)

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

// onCommand executes LOCK/UNLOCK/OPEN. Every actuation runs under an
// absolute deadline of At+CommandMaxAge, so a command is never executed
// late. It is sent through the cloud only when the bridge provably did not
// receive it; anything the bridge may have acted on is verified, never
// resent. Refreshes triggered by a failure run after the command resolved.
func (s *Supervisor) onCommand(ctx context.Context, m CommandMsg) {
	now := s.d.Now()
	deadline := m.At.Add(s.t.CommandMaxAge)
	if !now.Before(deadline) {
		s.commandFailed(m.Command, model.FailExpired, errors.New("command older than 10s when dequeued"))
		return
	}
	// The deadline is computed on the supervisor clock; contexts expire on
	// the real clock, so pass the remaining time.
	cctx, cancel := context.WithTimeout(ctx, deadline.Sub(now))
	defer cancel()
	switch s.mode {
	case model.ModeOffline:
		s.commandFailed(m.Command, model.FailOffline, errors.New("the lock is offline"))
	case model.ModeCloud:
		s.sendViaCloud(cctx, m.Command)
	default:
		s.sendViaBridge(ctx, cctx, m.Command)
	}
}

func (s *Supervisor) sendViaBridge(ctx, cctx context.Context, c model.Command) {
	now := s.d.Now()
	err := s.bridgeCommand(cctx, c)
	switch {
	case err == nil:
		s.httpFailures = 0
		s.awaitConfirm(now, c.Target())
	case errors.Is(err, loqed.ErrUnreachable), errors.Is(err, errNoBridge):
		// Never delivered: the cloud may send it.
		s.log.Warn("bridge did not receive the command; sending it via the cloud", "command", c, "err", err)
		s.sendViaCloud(cctx, c)
		if !errors.Is(err, errNoBridge) {
			s.httpFailure(ctx, err)
		}
	case errors.Is(err, loqed.ErrUnauthorized):
		// The bridge rejected the signature, so nothing happened.
		s.log.Warn("bridge rejected the command signature; sending it via the cloud", "command", c)
		s.sendViaCloud(cctx, c)
		if !s.refreshAndRebuild(ctx, ReasonUnauthorized) && s.bridge == nil {
			s.enterCloud(ctx)
		}
	default:
		// The bridge may have acted (timeout after sending, reset, 5xx):
		// do not resend; check the outcome right away.
		s.commandFailed(c, failClass(err), err)
		s.awaitConfirm(now, c.Target())
		s.confirmAt, s.confirmViaCloud = now, true
		s.httpFailure(ctx, err)
	}
}

func (s *Supervisor) bridgeCommand(ctx context.Context, c model.Command) error {
	if s.bridge == nil && !s.ensureBridge() {
		return errNoBridge
	}
	rc, cancel := s.reqCtx(ctx)
	defer cancel()
	return s.bridge.Command(rc, actionFor(c))
}

// sendViaCloud sends the command through the cloud within the command's
// deadline and schedules one confirmation poll (also when the outcome is
// unknown: the command may have run).
func (s *Supervisor) sendViaCloud(cctx context.Context, c model.Command) {
	if err := cctx.Err(); err != nil {
		s.commandFailed(c, model.FailExpired, err)
		return
	}
	err := s.d.Cloud.Command(cctx, s.id, c.Target())
	now := s.d.Now()
	if err == nil {
		moving := c.Moving()
		s.state.Lock = &moving
		s.publish()
		s.scheduleCloudConfirm(now, c.Target())
		return
	}
	class := failClass(err)
	s.commandFailed(c, class, err)
	if class == model.FailNoResponse {
		s.scheduleCloudConfirm(now, c.Target())
	}
}
```

- [ ] **Step 4: Run tests**

Run: `gofmt -l internal/gateway && go test ./internal/gateway/ -v -race`
Expected: PASS (65 tests).

- [ ] **Step 5: Commit**

```bash
git add internal/gateway
git commit -m "gateway: execute lock commands with deadlines and safe fallback

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 12: Cloud webhook events, supervisor manager, real-hub tests

**Files:**
- Replace: `internal/gateway/cloudevents.go`
- Create: `internal/gateway/manager.go`, `internal/gateway/cloudevents_test.go`, `internal/gateway/manager_test.go`, `internal/gateway/integration_test.go`

**Interfaces:**
- Consumes: `cloud.WebhookEvent` kinds (`BoltState`, `Jammed`), `recordEvent`, `onReached`, `keyName`, `lastBridgeEvent`, `webhookOK`, `nextPollTime`, `Timing.EnrichWindow`.
- Produces:
  - `onCloudEvent(ctx, cloud.WebhookEvent)`, `enrichFromCloud(now, cloud.WebhookEvent)` (same event type **and** key id within 30 s), `sameKey(a, b *int) bool`
  - `var gateway.ErrUnknownLock, ErrBusy`
  - `func gateway.NewManager(sups []*Supervisor) *Manager`; `(*Manager).Run(ctx)`, `Remove(ids []string) []string` (stops those supervisors and waits until they exited; returns the ids that were running), `DeliverBridgeEvent(lockID string, ev bridge.Event) error`, `DeliverCloudEvent(ev cloud.WebhookEvent) error`, `DeliverCommand(lockID string, c model.Command, at time.Time) error`, `BridgeKey(lockID string) ([]byte, bool)`, `Health() map[string]Health`

Cloud events drive state in cloud mode, and in local mode while the bridge webhook is not registered; otherwise they only enrich. With working cloud webhooks the next poll moves to the reconcile cadence counted from the last successful poll, so events never postpone it. `integration_test.go` runs the supervisor against the real `CloudHub`, `Budget` and `Refresher` (only the HTTP API is faked).

- [ ] **Step 1: Write failing tests**

`internal/gateway/cloudevents_test.go`:

```go
package gateway

import (
	"errors"
	"testing"
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/cloud"
	"github.com/t3hk0d3/go-loqed/internal/config"
	"github.com/t3hk0d3/go-loqed/internal/model"
)

func cloudReached(name string) CloudEventMsg {
	return CloudEventMsg{Event: cloud.WebhookEvent{Kind: cloud.KindStateReached, LockID: "lock1", EventType: "STATE_CHANGED_NIGHT_LOCK",
		BoltState: loqed.BoltNightLock, RequestedState: loqed.BoltNightLock, KeyLocalID: model.Ptr(3), KeyNameUser: name}}
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
	for range 60 {
		h.advance(time.Minute)
	}
	if len(h.cloud.calls) != calls {
		t.Fatal("with cloud webhooks working, background polling must stop")
	}
}

// Cloud events must not postpone the reconcile poll forever.
func TestReconcilePollIsScheduledFromLastPoll(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{}, func(d *Deps) { d.CloudWebhooks = true })
	h.start()
	h.toCloud()
	h.send(cloudReached(""))
	polls := len(h.cloud.calls)
	for range 25 { // an event every hour for a day
		h.advance(time.Hour)
		h.send(cloudReached(""))
	}
	if len(h.cloud.calls) <= polls {
		t.Fatal("the daily reconcile poll never ran")
	}
}

func TestCloudEventBringsOfflineLockBackToCloud(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.toCloud()
	h.cloudProbeErr = loqed.ErrUnreachable
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
	h.send(reached("STATE_CHANGED_NIGHT_LOCK", model.Ptr(3)))
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

func TestCloudEventWithOtherKeyDoesNotEnrich(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.start()
	h.send(reached("STATE_CHANGED_NIGHT_LOCK", model.Ptr(5)))
	h.send(cloudReached("Someone else")) // key 3
	if h.state().LastKeyName != nil {
		t.Fatal("a different key must not name this event")
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
	h.send(reached("STATE_CHANGED_NIGHT_LOCK", model.Ptr(3)))
	h.now = h.now.Add(31 * time.Second)
	h.send(cloudReached("late"))
	if h.state().LastKeyName != nil {
		t.Fatal("cloud event outside the 30s window must not enrich")
	}
}

// Without a registered bridge webhook, cloud events are the only push
// source in local mode.
func TestCloudEventsDriveStateWhileBridgeWebhookIsMissing(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.bridge.listErr = errors.New("no free webhook slots")
	h.start()
	h.send(cloudReached(""))
	if h.s.mode != model.ModeLocal || h.lock() != "LOCKED" {
		t.Fatalf("mode %s lock %s", h.s.mode, h.lock())
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
	"context"
	"errors"
	"testing"
	"time"

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

func TestManagerRemoveStopsSupervisor(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	h.s.d.Now = time.Now // Run uses the real ticker
	m := NewManager([]*Supervisor{h.s})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { m.Run(ctx); close(done) }()
	deadline := time.Now().Add(5 * time.Second)
	for len(m.Health()) == 1 && m.Health()["lock1"].Mode != model.ModeLocal {
		if time.Now().After(deadline) {
			t.Fatal("supervisor did not start")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := m.Remove([]string{"lock1", "nope"}); len(got) != 1 {
		t.Fatalf("removed %v", got)
	}
	if err := m.DeliverCommand("lock1", model.CommandOpen, time.Now()); !errors.Is(err, ErrUnknownLock) {
		t.Fatalf("got %v", err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run must return once every supervisor stopped")
	}
}
```

`internal/gateway/integration_test.go`:

```go
package gateway

import (
	"context"
	"log/slog"
	"testing"
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/cloud"
	"github.com/t3hk0d3/go-loqed/internal/config"
	"github.com/t3hk0d3/go-loqed/internal/model"
	"github.com/t3hk0d3/go-loqed/internal/store"
)

// lockAPI is a fake Lock API: commands move the bolt, reads report it.
type lockAPI struct {
	bolt  loqed.BoltState
	reads int
}

func (a *lockAPI) ListLocks(context.Context) ([]cloud.Lock, error) {
	a.reads++
	id := 1
	return []cloud.Lock{{ID: "lock1", BoltState: a.bolt, Online: model.Ptr(true), BridgeIP: "192.0.2.10",
		LocalID: &id, KeySecret: "SGFsbG8gd2VyZWxk", BridgeKey: "Ym9uam91ciBtb25kZQ=="}}, nil
}

func (a *lockAPI) Command(_ context.Context, _ string, s loqed.BoltState) error {
	a.bolt = s
	return nil
}

// realCloud wires the harness to a real CloudHub, Budget and Refresher.
func realCloud(t *testing.T, h *harness, api *lockAPI) (*CloudHub, *Refresher) {
	t.Helper()
	clock := func() time.Time { return h.now }
	hub := NewCloudHub(NewBudget(10, 12*time.Hour, clock, store.BudgetState{}, nil), &fakeTokens{token: "tok"},
		func(string) CloudAPI { return api }, clock, slog.New(slog.DiscardHandler))
	st := newStore(t)
	ref := NewRefresher(hub, st, clock)
	h.s.d.Cloud = hub
	h.s.d.Refresh = ref.Refresh
	return hub, ref
}

// UNLOCK in cloud mode: the confirmation poll 5 s later must not be
// answered with the lock list cached just before the command.
func TestRealHubConfirmShowsCommandResult(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	api := &lockAPI{bolt: loqed.BoltNightLock}
	realCloud(t, h, api)
	h.start()
	h.toCloud()
	if h.lock() != "LOCKED" {
		t.Fatalf("lock %s", h.lock())
	}
	h.run(10 * time.Second) // well inside the 30 s sharing window
	h.command(model.CommandUnlock)
	h.run(6 * time.Second)
	if h.lock() != "UNLOCKED" || h.state().StateStale {
		t.Fatalf("lock %s stale %v reads %d", h.lock(), h.state().StateStale, api.reads)
	}
}

// A bridge whose Wi-Fi flaps every few minutes for 12 h must not drain the
// shared budget: refresh backoff and the reserve leave room for confirms.
func TestRealHubFlappingBridgeKeepsBudget(t *testing.T) {
	h := newHarness(t, testRecord(), config.LockSetting{})
	api := &lockAPI{bolt: loqed.BoltNightLock}
	hub, _ := realCloud(t, h, api)
	h.start()
	for range 48 { // 12 h: 3 min down, 12 min up
		h.probeErr = loqed.ErrUnreachable
		for range 3 {
			h.advance(time.Minute)
		}
		h.probeErr = nil
		for range 12 {
			h.advance(time.Minute)
		}
	}
	if hub.Budget().Remaining() < 1 {
		t.Fatalf("budget drained by a flapping bridge: %d reads", api.reads)
	}
	if api.reads > 10 {
		t.Fatalf("%d reads in 12h exceed the budget", api.reads)
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/gateway/`
Expected: FAIL — `undefined: NewManager`.

- [ ] **Step 3: Replace `cloudevents.go`**

`internal/gateway/cloudevents.go`:

```go
package gateway

import (
	"context"
	"strings"
	"time"

	"github.com/t3hk0d3/go-loqed/cloud"
	"github.com/t3hk0d3/go-loqed/internal/model"
)

// onCloudEvent handles a cloud webhook. While the bridge webhook is
// registered in local mode it only adds a key name to a matching recent
// bridge event; otherwise it drives state.
func (s *Supervisor) onCloudEvent(_ context.Context, e cloud.WebhookEvent) {
	now := s.d.Now()
	s.lastCloudEventAt = now
	if s.mode == model.ModeLocal && s.webhookOK {
		s.enrichFromCloud(now, e)
		return
	}
	if s.mode == model.ModeOffline {
		s.setMode(model.ModeCloud) // a cloud webhook proves the cloud works
		s.bridgeProbeSchedule(now)
		s.nextCloudProbe = now.Add(s.t.Liveness)
	}
	if s.mode == model.ModeCloud {
		// Push works: drop to the reconcile cadence, counted from the last
		// successful poll (so events never postpone it).
		s.nextCloudPoll = s.nextPollTime(now)
	}
	switch e.Kind {
	case cloud.KindStateReached:
		s.state.LockOnline = true
		s.onReached(now, e.BoltState, e.Jammed)
		s.recordEvent(now, e.EventType, e.KeyLocalID, e.KeyNameUser, model.FromStateReached(e.EventType), false)
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
		if e.Online != nil {
			s.state.LockOnline = *e.Online
		}
		s.publish()
	}
}

// enrichFromCloud adds key_name_user to the bridge event it describes:
// same event type and key id, within EnrichWindow. It never emits an event.
func (s *Supervisor) enrichFromCloud(now time.Time, e cloud.WebhookEvent) {
	if e.Kind != cloud.KindStateReached && e.Kind != cloud.KindGoToState {
		return
	}
	last := s.lastBridgeEvent
	if last == nil || e.KeyNameUser == "" || s.state.LastKeyName != nil ||
		!strings.EqualFold(last.eventType, e.EventType) || now.Sub(last.at) > s.t.EnrichWindow ||
		!sameKey(last.keyID, model.NormalizeKeyID(e.KeyLocalID)) {
		return
	}
	name := e.KeyNameUser
	s.state.LastKeyName = &name
	s.publish()
}

func sameKey(a, b *int) bool {
	return (a == nil && b == nil) || (a != nil && b != nil && *a == *b)
}
```

- [ ] **Step 4: Implement `manager.go`**

`internal/gateway/manager.go`:

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

type running struct {
	s      *Supervisor
	cancel context.CancelFunc
	done   chan struct{}
}

// Manager routes webhooks and commands to lock supervisors and stops the
// supervisors of locks removed from the account at runtime.
type Manager struct {
	mu   sync.Mutex
	sups map[string]*running
	wg   sync.WaitGroup
}

func NewManager(sups []*Supervisor) *Manager {
	m := &Manager{sups: make(map[string]*running, len(sups))}
	for _, s := range sups {
		m.sups[s.ID()] = &running{s: s, done: make(chan struct{})}
	}
	return m
}

// Run runs every supervisor until ctx is cancelled.
func (m *Manager) Run(ctx context.Context) {
	m.mu.Lock()
	for _, r := range m.sups {
		sctx, cancel := context.WithCancel(ctx)
		r.cancel = cancel
		m.wg.Go(func() {
			defer close(r.done)
			r.s.Run(sctx)
		})
	}
	m.mu.Unlock()
	m.wg.Wait()
}

// Remove stops the supervisors of ids and waits until they have exited, so
// they can no longer publish. It returns the ids that were running.
func (m *Manager) Remove(ids []string) []string {
	var stopped []*running
	var out []string
	m.mu.Lock()
	for _, id := range ids {
		if r, ok := m.sups[id]; ok {
			delete(m.sups, id)
			stopped = append(stopped, r)
			out = append(out, id)
		}
	}
	m.mu.Unlock()
	for _, r := range stopped {
		if r.cancel != nil {
			r.cancel()
			<-r.done
		}
	}
	return out
}

func (m *Manager) get(lockID string) (*Supervisor, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.sups[lockID]
	if !ok {
		return nil, false
	}
	return r.s, true
}

func (m *Manager) deliver(lockID string, msg any) error {
	s, ok := m.get(lockID)
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
	s, ok := m.get(lockID)
	if !ok {
		return nil, false
	}
	return s.BridgeKey()
}

func (m *Manager) Health() map[string]Health {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]Health, len(m.sups))
	for id, r := range m.sups {
		out[id] = r.s.Health()
	}
	return out
}
```

- [ ] **Step 5: Run tests**

Run: `gofmt -l internal/gateway && go vet ./internal/gateway/ && go test ./internal/gateway/ -v -race -count=3`
Expected: PASS (77 tests, three runs).

- [ ] **Step 6: Commit**

```bash
git add internal/gateway
git commit -m "gateway: handle cloud webhooks, add the supervisor manager

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
  - `type webhook.Options struct{ Sink Sink; CloudSecret string; MQTTDownFor func() time.Duration; Now func() time.Time; Log *slog.Logger }` (empty `CloudSecret` = cloud route disabled)
  - `const webhook.MQTTGrace = 5 * time.Minute`; `type webhook.HealthReport struct{ MQTTConnected bool; Locks map[string]gateway.Health }` (JSON `mqtt_connected`, `locks`)
  - `func webhook.NewHandler(o Options) http.Handler` — routes `POST /webhook/{id}`, `POST /cloud/{secret}`, `GET /healthz` (503 only after MQTT has been down for more than `MQTTGrace`)
  - `func webhook.PrivateURL(base string, port int, lockID, bridgeIP string) (string, error)`; `func webhook.SourceIP(bridgeIP string) (net.IP, error)`; `func webhook.LikelyContainerAddress(local net.IP, bridgeIP string) bool`; `func webhook.CloudURL(publicBase, secret string) string`

Notes: a real `net/http` server canonicalizes incoming header names, so the handler reads `Hash`/`Timestamp` with `Header.Get`; tests must set headers with `Header.Set` (a raw `req.Header["HASH"]` only exists in hand-built requests). Bodies are capped at 64 KiB; cloud webhook bodies are never logged.

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
	"net"
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

func handler(sink *fakeSink, mqttDown time.Duration, cloudSecret string) http.Handler {
	return webhook.NewHandler(webhook.Options{Sink: sink, CloudSecret: cloudSecret,
		MQTTDownFor: func() time.Duration { return mqttDown },
		Now:         func() time.Time { return now }, Log: slog.New(slog.DiscardHandler)})
}

func signed(body string, ts int64) (string, string) {
	h := sha256.New()
	h.Write([]byte(body))
	h.Write(binary.BigEndian.AppendUint64(nil, uint64(ts)))
	h.Write([]byte("bonjour monde"))
	return hex.EncodeToString(h.Sum(nil)), strconv.FormatInt(ts, 10)
}

// post sends a request through the handler. A real server canonicalizes
// incoming header names (the bridge sends TIMESTAMP/HASH), so set them
// canonically here too.
func post(h http.Handler, path, body string, header map[string]string) int {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	for k, v := range header {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec.Code
}

const reached = `{"requested_state":"NIGHT_LOCK","event_type":"STATE_CHANGED_NIGHT_LOCK","key_local_id":255}`

func TestBridgeWebhook(t *testing.T) {
	sink := &fakeSink{}
	h := handler(sink, 0, "")
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
		sink := &fakeSink{busy: c.busy}
		if code := post(handler(sink, 0, ""), c.path, reached, c.header); code != c.want {
			t.Errorf("%s: got %d want %d", c.name, code, c.want)
		}
		if c.want != 200 && len(sink.bridgeEvents) != 0 {
			t.Errorf("%s: rejected event was delivered", c.name)
		}
	}
}

func TestBridgeWebhookBodyLimit(t *testing.T) {
	big := strings.Repeat("x", 70<<10)
	hash, ts := signed(big, now.Unix())
	if code := post(handler(&fakeSink{}, 0, ""), "/webhook/lock1", big, map[string]string{"HASH": hash, "TIMESTAMP": ts}); code != 413 {
		t.Fatalf("code %d", code)
	}
}

func TestCloudWebhook(t *testing.T) {
	sink := &fakeSink{}
	h := handler(sink, 0, secret)
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
	if code := post(handler(sink, 0, ""), "/cloud/"+secret, body, nil); code != 404 {
		t.Fatalf("cloud route must be off without a secret: %d", code)
	}
}

func TestHealthzToleratesShortMQTTOutages(t *testing.T) {
	cases := []struct {
		down time.Duration
		code int
		conn bool
	}{
		{0, 200, true},
		{2 * time.Minute, 200, false}, // broker restart: no watchdog restart
		{6 * time.Minute, 503, false},
	}
	for _, c := range cases {
		rec := httptest.NewRecorder()
		handler(&fakeSink{}, c.down, "").ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
		var body webhook.HealthReport
		if rec.Code != c.code || json.Unmarshal(rec.Body.Bytes(), &body) != nil ||
			body.MQTTConnected != c.conn || body.Locks["lock1"].Mode != model.ModeLocal {
			t.Fatalf("down=%v code %d body %s", c.down, rec.Code, rec.Body)
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

func TestLikelyContainerAddress(t *testing.T) {
	if !webhook.LikelyContainerAddress(net.ParseIP("172.17.0.2"), "192.168.1.50") {
		t.Fatal("docker bridge address towards a LAN bridge")
	}
	if webhook.LikelyContainerAddress(net.ParseIP("192.168.1.10"), "192.168.1.50") ||
		webhook.LikelyContainerAddress(net.ParseIP("172.20.0.5"), "172.20.0.9:80") {
		t.Fatal("false positive")
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/webhook/`
Expected: FAIL — `no non-test Go files in …/internal/webhook`.

- [ ] **Step 3: Implement `handler.go`**

`internal/webhook/handler.go`:

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

// MQTTGrace is how long MQTT may be down before /healthz reports 503, so a
// broker restart does not make the add-on watchdog restart the gateway.
const MQTTGrace = 5 * time.Minute

type Sink interface {
	BridgeKey(lockID string) ([]byte, bool)
	DeliverBridgeEvent(lockID string, ev bridge.Event) error
	DeliverCloudEvent(ev cloud.WebhookEvent) error
	Health() map[string]gateway.Health
}

type Options struct {
	Sink        Sink
	CloudSecret string               // empty disables POST /cloud/{secret}
	MQTTDownFor func() time.Duration // 0 while connected
	Now         func() time.Time
	Log         *slog.Logger
}

// HealthReport is the /healthz body.
type HealthReport struct {
	MQTTConnected bool                      `json:"mqtt_connected"`
	Locks         map[string]gateway.Health `json:"locks"`
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
		// Only reachable with a valid HASH, so this is a real clock problem.
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
	ev, err := cloud.ParseWebhook(body) // the body is never logged: it holds personal data
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
	var down time.Duration
	if o.MQTTDownFor != nil {
		down = o.MQTTDownFor()
	}
	code := http.StatusOK
	if down > MQTTGrace {
		code = http.StatusServiceUnavailable
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(HealthReport{MQTTConnected: down == 0, Locks: o.Sink.Health()})
}
```

- [ ] **Step 4: Implement `urls.go`**

`internal/webhook/urls.go`:

```go
package webhook

import (
	"fmt"
	"net"
	"net/netip"
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
	defer func() { _ = conn.Close() }()
	return conn.LocalAddr().(*net.UDPAddr).IP, nil
}

var dockerNets = netip.MustParsePrefix("172.16.0.0/12")

// LikelyContainerAddress reports whether local looks like a Docker bridge
// network address that the LOQED bridge (on the LAN) cannot reach.
func LikelyContainerAddress(local net.IP, bridgeIP string) bool {
	l, ok := netip.AddrFromSlice(local)
	if !ok {
		return false
	}
	host := bridgeIP
	if h, _, err := net.SplitHostPort(bridgeIP); err == nil {
		host = h
	}
	b, err := netip.ParseAddr(host)
	if err != nil {
		return false
	}
	return dockerNets.Contains(l.Unmap()) && !dockerNets.Contains(b.Unmap())
}

func CloudURL(publicBase, secret string) string {
	return strings.TrimRight(publicBase, "/") + "/cloud/" + secret
}
```

- [ ] **Step 5: Run tests**

Run: `gofmt -l internal/webhook && go test ./internal/webhook/ -v -race`
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
- Create: `internal/app/app.go`, `internal/app/app_test.go`, `cmd/loqed-mqtt/main.go`, `cmd/loqed-mqtt/main_test.go`

**Interfaces:**
- Consumes: every package above.
- Produces:
  - `type app.Options struct{ Config config.Config; Log *slog.Logger; Version string; CloudBaseURL, PortalBaseURL string; Now func() time.Time; Ready func(webhookAddr string) }` (empty base URLs = production; `Ready` is a test hook)
  - `func app.Run(ctx context.Context, o Options) error` — returns nil when ctx is cancelled (also during startup), the listener's error if the webhook server dies, or a startup error.
  - Binary `loqed-mqtt [--config file]`, subcommand `loqed-mqtt healthcheck [--config file]`; `main.version` set via `-ldflags "-X main.version=..."`; `log_format: json` selects the JSON handler.

Startup (spec §5.4): open cache (warn on corrupt/unknown version) → install id → token resolver (+ portal minter only when e-mail and password are set) → budget restored from the cache and saved on every change → hub/refresher → refresh when the cache is missing/corrupt, the token hash differs, an allow-listed lock is missing **and** the cache is older than 12 h, or `cache_max_age` passed (fall back to the cache on cloud failure; fail if there is no cache; a cache write failure is reported as such) → select locks, warn about unmatched allow-list names and `lock_settings` → listen → MQTT, clearing retained topics of `published_ids` (and cached locks) no longer selected, then saving `published_ids` → supervisors (cloud probe = TCP to the cloud host) → HTTP server with read/write/idle timeouts → log the cloud webhook URL once. At runtime, a refresh that drops a lock stops its supervisor (`Manager.Remove`) and clears its retained topics; `cache_max_age` is re-checked hourly.

- [ ] **Step 1: Write the failing end-to-end tests**

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
```

`cmd/loqed-mqtt/main_test.go`:

```go
package main

import "testing"

func TestHealthURL(t *testing.T) {
	cases := map[string]string{
		":8099":            "http://127.0.0.1:8099/healthz",
		"0.0.0.0:8099":     "http://127.0.0.1:8099/healthz",
		"[::]:8099":        "http://127.0.0.1:8099/healthz",
		"192.0.2.5:9000":   "http://192.0.2.5:9000/healthz",
		"[2001:db8::1]:80": "http://[2001:db8::1]:80/healthz",
	}
	for in, want := range cases {
		if got, err := healthURL(in); err != nil || got != want {
			t.Errorf("%s: %q %v", in, got, err)
		}
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/app/ ./cmd/...`
Expected: FAIL — `no non-test Go files`.

- [ ] **Step 3: Implement `internal/app/app.go`**

`internal/app/app.go`:

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
	"net/url"
	"slices"
	"sync"
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
	Version       string
	CloudBaseURL  string // empty = production
	PortalBaseURL string // empty = production
	Now           func() time.Time
	Ready         func(webhookAddr string) // test hook
}

// budgetWindow is LOQED's documented rate-limit window.
const budgetWindow = 12 * time.Hour

// missingLockRefreshAge: an allow-listed lock missing from a cache younger
// than this does not trigger a refresh (a typo must not spend a call on
// every restart).
const missingLockRefreshAge = 12 * time.Hour

// Run starts the gateway and blocks until ctx is cancelled (returns nil)
// or a fatal error occurs.
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
	switch status {
	case store.StatusCorrupt:
		log.Warn("the credential cache is unreadable; rebuilding it from the cloud", "path", cfg.CachePath)
	case store.StatusUnknownVersion:
		log.Warn("the credential cache was written by another version; rebuilding it from the cloud", "path", cfg.CachePath)
	}
	before := st.Snapshot()
	installID, err := st.InstallID()
	if err != nil {
		log.Error("cannot write the credential cache; running from memory", "err", err)
	}

	var minter auth.Minter
	if cfg.CanMint() {
		minter = auth.NewPortalMinter(portal.New(portal.WithBaseURL(portalBase)), cfg.CloudEmail, cfg.CloudPassword,
			auth.TokenName(installID), log)
	}
	resolver := auth.NewResolver(cfg.CloudToken, cfg.CloudEmail, minter, st, now, log)
	var saveWarn sync.Once
	budget := gateway.NewBudget(cfg.CloudBudget, budgetWindow, now, before.Budget, func(b store.BudgetState) {
		if err := st.Update(func(c *store.Cache) { c.Budget = b }); err != nil {
			saveWarn.Do(func() { log.Error("cannot persist the cloud request budget", "err", err) })
		}
	})
	hub := gateway.NewCloudHub(budget, resolver,
		func(tok string) gateway.CloudAPI { return cloud.New(tok, cloud.WithBaseURL(cloudBase)) }, now, log)
	refresher := gateway.NewRefresher(hub, st, now)

	if err := initialRefresh(ctx, cfg, st, status, resolver, refresher, now, log); err != nil {
		if ctx.Err() != nil {
			return nil // stopped during startup
		}
		return err
	}
	all := st.Snapshot().Locks
	selected, missing := gateway.Select(all, cfg.Locks)
	for _, m := range missing {
		log.Warn("a lock from the locks allow-list is not on the account", "lock", m)
	}
	for _, k := range gateway.UnmatchedSettings(cfg.LockSettings, all) {
		log.Warn("lock_settings entry matches no lock id or name", "entry", k)
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
		HAEnabled: cfg.HomeAssistant.Enabled, Version: o.Version, Now: now,
	}, log)
	published := newPublished(selected)
	mq.SetLocks(published.infos(), removedIDs(before, selected))
	savePublished(st, published.ids(), log)
	mq.Start()
	defer mq.Close()

	cloudHost := hostPort(cloudBase)
	deps := gateway.Deps{
		Publisher: mq, Cloud: hub, Refresh: refresher.Refresh,
		NewBridge: func(rec store.LockRecord) (gateway.BridgeAPI, error) {
			if !rec.HasLocalCredentials() {
				return nil, errors.New("lock has no usable local credentials")
			}
			c, err := bridge.New(rec.BridgeIP, bridge.Credentials{BridgeKey: rec.BridgeKey, KeySecret: rec.KeySecret,
				LocalKeyID: uint8(*rec.LocalID)}, bridge.WithClock(now)) //nolint:gosec // G115: HasLocalCredentials checks 0..255
			if err != nil {
				return nil, err
			}
			return c, nil
		},
		Probe:      gateway.TCPProbe,
		ProbeCloud: func(ctx context.Context) error { return gateway.TCPProbe(ctx, cloudHost) },
		WebhookURL: func(rec store.LockRecord) (string, error) {
			return webhook.PrivateURL(cfg.Webhook.PrivateURL, port, rec.ID, rec.BridgeIP)
		},
		CloudWebhooks: cfg.Webhook.PublicURL != "",
		Now:           now,
		Log:           log,
	}
	timing := gateway.DefaultTiming(cfg.LivenessInterval.D(), cfg.ReconcileInterval.D(), budget.Spacing())
	sups := make([]*gateway.Supervisor, 0, len(selected))
	for _, r := range selected {
		sups = append(sups, gateway.NewSupervisor(r, gateway.SettingFor(cfg.LockSettings, r), deps, timing))
		warnContainerNetwork(cfg, r, log)
	}
	manager := gateway.NewManager(sups)
	refresher.OnRemoved = func(ids []string) {
		// Runs on a supervisor goroutine; stopping others must not wait on it.
		go func() {
			gone := manager.Remove(ids)
			if len(gone) == 0 {
				return
			}
			log.Info("locks were removed from the account", "lock_ids", gone)
			mq.SetLocks(published.remove(gone), gone)
			savePublished(st, published.ids(), log)
		}()
	}

	srv := &http.Server{
		Handler: webhook.NewHandler(webhook.Options{Sink: manager, CloudSecret: cloudSecret,
			MQTTDownFor: mq.DisconnectedFor, Now: now, Log: log}),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	runCtx, stop := context.WithCancelCause(ctx)
	defer stop(nil)
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			stop(fmt.Errorf("webhook server: %w", err))
		}
	}()
	if cloudSecret != "" {
		log.Info("register this URL as the webhook in the API section of app.loqed.com", "url", webhook.CloudURL(cfg.Webhook.PublicURL, cloudSecret))
	}
	go forwardCommands(runCtx, mq, manager, log)
	go refreshByAge(runCtx, refresher, cfg.CacheMaxAge.D(), log)
	if o.Ready != nil {
		o.Ready(ln.Addr().String())
	}

	manager.Run(runCtx)
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	if cause := context.Cause(runCtx); ctx.Err() == nil && cause != nil {
		return cause
	}
	return nil
}

func forwardCommands(ctx context.Context, mq *hass.Client, m *gateway.Manager, log *slog.Logger) {
	for {
		select {
		case <-ctx.Done():
			return
		case c := <-mq.Commands():
			if err := m.DeliverCommand(c.LockID, c.Command, c.At); err != nil {
				log.Warn("command not delivered", "lock_id", c.LockID, "err", err)
			}
		}
	}
}

// refreshByAge applies cache_max_age while running (checked hourly).
func refreshByAge(ctx context.Context, r *gateway.Refresher, maxAge time.Duration, log *slog.Logger) {
	if maxAge <= 0 {
		return
	}
	t := time.NewTicker(time.Hour)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if ran, err := r.RefreshIfOlder(ctx, maxAge); ran && err != nil {
				log.Warn("scheduled credential refresh failed", "err", err)
			}
		}
	}
}

func initialRefresh(ctx context.Context, cfg config.Config, st *store.Store, status store.Status,
	resolver *auth.Resolver, refresher *gateway.Refresher, now func() time.Time, log *slog.Logger) error {
	snap := st.Snapshot()
	tok, err := resolver.Token(ctx)
	if errors.Is(err, auth.ErrNoToken) && len(snap.Locks) == 0 {
		return err
	}
	if err != nil {
		if len(snap.Locks) > 0 {
			log.Warn("no usable LOQED token; starting from the credential cache", "err", err)
			return nil
		}
		return fmt.Errorf("cannot start: no credential cache and no usable LOQED token: %w", err)
	}
	age := now().Sub(snap.FetchedAt)
	need := status != store.StatusLoaded ||
		snap.TokenSHA256 != store.TokenHash(tok) ||
		(missingFromCache(snap, cfg.Locks) && age > missingLockRefreshAge) ||
		(cfg.CacheMaxAge > 0 && age > cfg.CacheMaxAge.D())
	if !need {
		return nil
	}
	_, err = refresher.RefreshAll(ctx)
	switch {
	case errors.Is(err, store.ErrWrite):
		log.Error("lock data refreshed but the credential cache cannot be written; the next start needs the cloud again", "err", err)
		return nil
	case err != nil && len(snap.Locks) > 0:
		log.Warn("cloud refresh failed; starting from the credential cache", "err", err)
		return nil
	case err != nil:
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

// removedIDs lists ids that had retained topics (published_ids, or cached
// locks from before published_ids existed) and are no longer selected.
func removedIDs(before store.Cache, selected []store.LockRecord) []string {
	keep := make(map[string]bool, len(selected))
	for _, r := range selected {
		keep[r.ID] = true
	}
	var out []string
	seen := map[string]bool{}
	add := func(id string) {
		if !keep[id] && !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	for _, id := range before.PublishedIDs {
		add(id)
	}
	for _, r := range before.Locks {
		add(r.ID)
	}
	return out
}

// published tracks the locks currently exposed over MQTT.
type published struct {
	mu    sync.Mutex
	locks []hass.LockInfo
}

func newPublished(recs []store.LockRecord) *published {
	p := &published{}
	for _, r := range recs {
		p.locks = append(p.locks, hass.LockInfo{ID: r.ID, Name: r.Name, Model: r.ModelName, MacWifi: r.BridgeMacWifi})
	}
	return p
}

func (p *published) infos() []hass.LockInfo {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.locks)
}

func (p *published) ids() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, 0, len(p.locks))
	for _, l := range p.locks {
		out = append(out, l.ID)
	}
	return out
}

func (p *published) remove(ids []string) []hass.LockInfo {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.locks = slices.DeleteFunc(p.locks, func(l hass.LockInfo) bool { return slices.Contains(ids, l.ID) })
	return slices.Clone(p.locks)
}

func savePublished(st *store.Store, ids []string, log *slog.Logger) {
	if err := st.Update(func(c *store.Cache) { c.PublishedIDs = ids }); err != nil {
		log.Warn("cannot save the published lock list", "err", err)
	}
}

func warnContainerNetwork(cfg config.Config, r store.LockRecord, log *slog.Logger) {
	if cfg.Webhook.PrivateURL != "" || r.BridgeIP == "" {
		return
	}
	if ip, err := webhook.SourceIP(r.BridgeIP); err == nil && webhook.LikelyContainerAddress(ip, r.BridgeIP) {
		log.Warn("the bridge would call a container-internal address it cannot reach; use host networking or set webhook.private_url",
			"lock", r.Name, "address", ip.String())
	}
}

// hostPort returns host:port of a base URL (port 443 for https).
func hostPort(base string) string {
	u, err := url.Parse(base)
	if err != nil {
		return base
	}
	if u.Port() != "" {
		return u.Host
	}
	if u.Scheme == "http" {
		return net.JoinHostPort(u.Hostname(), "80")
	}
	return net.JoinHostPort(u.Hostname(), "443")
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

`cmd/loqed-mqtt/main.go`:

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
	log := newLogger(cfg)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := config.ResolveMQTT(ctx, &cfg, os.Getenv("SUPERVISOR_TOKEN"), config.SupervisorURL, &http.Client{Timeout: 10 * time.Second}, nil); err != nil {
		if ctx.Err() != nil {
			return 0
		}
		log.Error("cannot determine the MQTT broker", "err", err)
		return 1
	}
	log.Info("starting loqed-mqtt", "version", version)
	if err := app.Run(ctx, app.Options{Config: cfg, Log: log, Version: version}); err != nil {
		log.Error("loqed-mqtt stopped", "err", err)
		return 1
	}
	return 0
}

func newLogger(cfg config.Config) *slog.Logger {
	var level slog.Level
	_ = level.UnmarshalText([]byte(cfg.LogLevel))
	opts := &slog.HandlerOptions{Level: level}
	if cfg.LogFormat == "json" {
		return slog.New(slog.NewJSONHandler(os.Stderr, opts))
	}
	return slog.New(slog.NewTextHandler(os.Stderr, opts))
}

func loadConfig(path string) (config.Config, error) {
	return config.Load(config.Sources{OptionsFile: optionsFile, ConfigFile: path, Environ: os.Environ()})
}

// healthcheck is used by Docker HEALTHCHECK: distroless has no shell or curl.
// Docker passes no flags, so configure the listen address via the
// environment (LOQED_WEBHOOK__LISTEN) when it is not the default.
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
	url, err := healthURL(cfg.Webhook.Listen)
	if err != nil {
		return 1
	}
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Get(url) //nolint:gosec // G107: local health endpoint
	if err != nil {
		return 1
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}

// healthURL maps a listen address to a dialable URL: an empty or
// unspecified host means loopback.
func healthURL(listen string) (string, error) {
	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		return "", err
	}
	if ip := net.ParseIP(host); host == "" || (ip != nil && ip.IsUnspecified()) {
		host = "127.0.0.1"
	}
	return "http://" + net.JoinHostPort(host, port) + "/healthz", nil
}
```

- [ ] **Step 5: Run all tests, lint and build**

Run:

```bash
gofmt -l . && go vet ./... && go test -race ./...
go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.14.0 run ./...
go build -o /dev/null ./cmd/loqed-mqtt
```

Expected: no gofmt output; all packages PASS (app: 5 tests); `0 issues.`; build succeeds.

- [ ] **Step 6: Commit**

```bash
git add internal/app cmd
git commit -m "app: wire gateway, add loqed-mqtt command and end-to-end test

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 15: Docker image, Home Assistant add-on, CI, docs

**Files:**
- Create: `Dockerfile`, `.dockerignore`, `docker-compose.yml`, `addon/config.yaml`, `addon/DOCS.md`, `addon/translations/en.yaml`, `internal/config/addon_test.go`, `repository.yaml`, `README.md`, `.github/workflows/ci.yml`, `.github/workflows/release.yml`

**Interfaces:**
- Consumes: `cmd/loqed-mqtt` (`healthcheck` subcommand, `main.version`), config keys from Task 1.
- Produces: images `ghcr.io/t3hk0d3/loqed-mqtt:<version>` (standalone: `linux/amd64`, `linux/arm64`, `linux/arm/v7`) and `ghcr.io/t3hk0d3/loqed-mqtt-addon:<version>` (`linux/amd64`, `linux/arm64`); an add-on repository installable from `https://github.com/t3hk0d3/go-loqed`.

Why this shape (current Supervisor docs): since Supervisor 2026.04 `build.yaml` is ignored and `BUILD_FROM` is no longer passed, so the add-on uses a **prebuilt** image (`image:`), built from the `addon` Dockerfile target, which runs as root because the Supervisor's `/data` is root-owned. Only `amd64` and `aarch64` are valid add-on architectures. Options nest at most two levels, so `key_names` is a `"1=Alice,3=Bob"` string; every nested option has a default; `webhook.listen` is not exposed (the watchdog and host networking assume 8099). The standalone target creates `/data` owned by uid 65532, so new named volumes are writable.

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
    go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/loqed-mqtt ./cmd/loqed-mqtt \
 && mkdir -p /out/data

# Home Assistant add-on image: runs as root because the Supervisor mounts
# /data owned by root. The Supervisor watchdog replaces HEALTHCHECK.
FROM gcr.io/distroless/static-debian12 AS addon
LABEL org.opencontainers.image.source=https://github.com/t3hk0d3/go-loqed
COPY --from=build /out/loqed-mqtt /loqed-mqtt
ENTRYPOINT ["/loqed-mqtt"]

# Standalone image (default target): distroless nonroot. /data is owned by
# uid 65532, and Docker copies that ownership into new named volumes.
FROM gcr.io/distroless/static-debian12:nonroot AS standalone
LABEL org.opencontainers.image.source=https://github.com/t3hk0d3/go-loqed
COPY --from=build /out/loqed-mqtt /loqed-mqtt
COPY --from=build --chown=65532:65532 /out/data /data
EXPOSE 8099
VOLUME /data
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s CMD ["/loqed-mqtt", "healthcheck"]
ENTRYPOINT ["/loqed-mqtt"]
```

`.dockerignore`:

```text
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
    # Host networking makes the auto-detected webhook URL the real LAN
    # address the bridge can reach. Without it, set LOQED_WEBHOOK__PRIVATE_URL.
    network_mode: host
    volumes:
      - loqed-data:/data
    environment:
      LOQED_CLOUD_TOKEN: "paste-your-personal-access-token"
      # or: LOQED_CLOUD_EMAIL / LOQED_CLOUD_PASSWORD
      LOQED_MQTT__URL: "tcp://192.168.1.10:1883"
      LOQED_MQTT__USERNAME: "loqed"
      LOQED_MQTT__PASSWORD: "change-me"
volumes:
  loqed-data:
```

Run (host with Docker):

```bash
docker build --build-arg VERSION=0.1.0-dev -t loqed-mqtt:dev .
docker build --target addon -t loqed-mqtt-addon:dev .
docker run --rm loqed-mqtt:dev --config /nonexistent.yaml; echo "exit=$?"
docker run --rm loqed-mqtt:dev healthcheck; echo "health exit=$?"
docker image inspect loqed-mqtt:dev --format '{{.Config.User}}'
docker run --rm -v loqed-test:/data --entrypoint "" busybox ls -ldn /data; docker volume rm loqed-test
```

Expected: both images build; `loqed-mqtt: invalid configuration: config: open /nonexistent.yaml: no such file or directory` and `exit=2`; `health exit=1` (no gateway running — proves the binary runs in distroless); user `65532`; the busybox line shows `/data` owned by `65532 65532` (Docker copied the image's ownership into the new volume).

- [ ] **Step 2: Add-on files and their test**

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
# Prebuilt image (no local build): the Supervisor pulls <image>:<version>.
image: ghcr.io/t3hk0d3/loqed-mqtt-addon
arch: [amd64, aarch64]
init: false
# The bridge must reach the gateway on the LAN; host networking makes the
# auto-detected webhook URL correct. Ports are not mapped with host_network.
host_network: true
services:
  - mqtt:need
watchdog: http://[HOST]:[PORT:8099]/healthz
# Every nested key has a default so options validate on a fresh install.
# Nesting is at most two levels (lock_settings → entry); key_names is a
# "1=Alice,3=Bob" string.
options:
  locks: []
  lock_settings: []
  webhook:
    private_url: ""
    public_url: ""
  mqtt:
    url: ""
    username: ""
    password: ""
    base_topic: loqed
  homeassistant:
    enabled: true
    discovery_prefix: homeassistant
  log_level: info
schema:
  cloud_token: password?
  cloud_email: str?
  cloud_password: password?
  locks: [str]
  lock_settings:
    - lock: str
      bridge_ip: str?
      bridge_key: password?
      key_secret: password?
      local_id: int(0,255)?
      key_names: str?
  cache_max_age: str?
  reconcile_interval: str?
  liveness_interval: str?
  cloud_budget: int(1,12)?
  webhook:
    private_url: str?
    public_url: str?
    cloud_secret: password?
  mqtt:
    url: str?
    username: str?
    password: password?
    base_topic: str?
  homeassistant:
    enabled: bool?
    discovery_prefix: str?
  log_level: list(debug|info|warn|error)?
  log_format: list(text|json)?
```

`addon/translations/en.yaml`:

```yaml
configuration:
  cloud_token:
    name: Personal access token
    description: Create one at https://integrations.loqed.com/personal-access-tokens. Leave empty to use email and password instead.
  cloud_email:
    name: LOQED email
    description: Used to create a personal access token automatically.
  cloud_password:
    name: LOQED password
    description: Only needed to create a token; can be removed after the first successful start.
  locks:
    name: Locks
    description: Names or ids of the locks to expose. Empty exposes every lock on the account.
  lock_settings:
    name: Lock settings
    description: Per-lock overrides (edit in YAML mode). key_names looks like "1=Alice,3=Bob".
  webhook:
    name: Webhooks
    description: private_url is the address the bridge calls (auto-detected when empty). public_url (scheme and host only) enables cloud webhooks.
  mqtt:
    name: MQTT
    description: Leave url empty to use the Mosquitto add-on.
  homeassistant:
    name: Home Assistant discovery
  log_level:
    name: Log level
  log_format:
    name: Log format
```

`addon/DOCS.md`:

````markdown
# LOQED MQTT Gateway

Exposes every LOQED lock on your account to Home Assistant through MQTT.
The gateway talks to your LOQED Bridge on the local network and falls back
to the LOQED cloud when the bridge is unreachable.

## Setup

1. Install and start the Mosquitto broker add-on.
2. Create a personal access token at
   https://integrations.loqed.com/personal-access-tokens and paste it into
   **Personal access token**. Alternatively, enter your LOQED email and
   password; the add-on then creates a token named `loqed-mqtt <id>`
   (the password can be removed after the first successful start).
3. Start the add-on. Each lock appears as a device with a lock, battery and
   signal sensors, a connection mode sensor, a last change reason sensor and
   a lock event entity.

## Lock settings

Optional per-lock overrides, edited in YAML mode:

```yaml
lock_settings:
  - lock: Front door          # lock name or id
    bridge_ip: 192.168.1.50   # pin the bridge address
    key_names: "1=Alice,3=Bob"
```

`key_names` maps the lock's key ids to names shown on lock events.
`bridge_key`, `key_secret` and `local_id` are only needed when the LOQED
cloud does not provide local credentials for a lock.

## Things to know

- **Time must be correct.** The bridge signs webhooks with a timestamp that
  must be within 10 seconds of this host's clock.
- **Lock events are best-effort.** The bridge occasionally loses a webhook.
  Use the lock entity, not the event entity, for automations that depend on
  whether the door is locked. Failed commands produce a `command_failed`
  event you can notify on.
- **Cloud limits.** LOQED blocks accounts that read lock status more than 12
  times in 12 hours. The gateway keeps cloud calls under `cloud_budget`
  (default 10, also across restarts), so in cloud mode without cloud
  webhooks the lock state can be more than an hour old. The `state_stale`
  attribute shows when it is.
- **Cloud webhooks (optional).** Set `webhook.public_url` (scheme and host
  only, for example `https://loqed.example.com`) to an address that reaches
  this add-on from the internet through a reverse proxy that forwards only
  the `/cloud/` path, unchanged, and does not log request paths (the path
  contains the secret). The add-on log shows the full URL once at startup;
  register it in the API section of https://app.loqed.com.
````

The add-on options must stay loadable by `config.Load`, and every schema key must be a real setting:

`internal/config/addon_test.go`:

```go
package config_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/t3hk0d3/go-loqed/internal/config"
)

// The add-on's options and schema must stay loadable by config.Load: the
// Supervisor writes the options (as JSON) to /data/options.json, and every
// schema key must be a real setting.
func TestAddonOptionsAndSchemaMatchConfig(t *testing.T) {
	raw, err := os.ReadFile("../../addon/config.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var addon struct {
		Options map[string]any `yaml:"options"`
		Schema  map[string]any `yaml:"schema"`
	}
	if err := yaml.Unmarshal(raw, &addon); err != nil {
		t.Fatal(err)
	}
	load := func(name string, v any) config.Config {
		t.Helper()
		b, _ := json.Marshal(v)
		p := filepath.Join(t.TempDir(), "options.json")
		if err := os.WriteFile(p, b, 0o600); err != nil {
			t.Fatal(err)
		}
		cfg, err := config.Load(config.Sources{OptionsFile: p})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		return cfg
	}
	cfg := load("options", addon.Options)
	cfg.CloudToken = "tok"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("default options do not validate: %v", err)
	}
	// Fill every schema key with a plausible value to prove the names exist.
	sample := map[string]any{}
	for k, v := range addon.Schema {
		switch v := v.(type) {
		case map[string]any:
			nested := map[string]any{}
			for nk := range v {
				nested[nk] = sampleFor(nk)
			}
			sample[k] = nested
		case []any:
			sample[k] = []any{}
		default:
			sample[k] = sampleFor(k)
		}
	}
	sample["lock_settings"] = []any{map[string]any{"lock": "x", "bridge_ip": "192.0.2.1", "bridge_key": "YQ==",
		"key_secret": "YQ==", "local_id": 1, "key_names": "1=Alice"}}
	load("schema", sample)
}

func sampleFor(key string) any {
	switch key {
	case "enabled":
		return true
	case "cloud_budget":
		return 10
	case "cache_max_age", "reconcile_interval", "liveness_interval":
		return "1h"
	case "log_level":
		return "info"
	case "log_format":
		return "json"
	default:
		return ""
	}
}
```

Run: `go test ./internal/config/ -run Addon -v`
Expected: PASS. (Adding an unknown key such as `bogus: str?` under `mqtt:` in the schema makes it fail.)

- [ ] **Step 3: README and CI**

`README.md`:

```markdown
# go-loqed

- **GoLoqed** (`bridge`, `cloud`, `cloud/portal`): a Go client for the LOQED
  local Bridge API, the cloud Lock API and the Integrations portal.
- **loqed-mqtt** (`cmd/loqed-mqtt`): a local-first MQTT gateway for LOQED
  locks with Home Assistant discovery and automatic cloud fallback.

## Running loqed-mqtt

Home Assistant OS: add this repository in *Settings → Add-ons → Add-on
store → Repositories* and install **LOQED MQTT Gateway** (amd64, aarch64).

Docker: see `docker-compose.yml` (host networking recommended). Without host
networking the auto-detected webhook address is the container's, which the
bridge cannot reach: set `LOQED_WEBHOOK__PRIVATE_URL` to
`http://<docker-host-ip>:8099` and publish port 8099. The Docker
`HEALTHCHECK` reads only environment and `/data/options.json`, so set a
non-default listen address with `LOQED_WEBHOOK__LISTEN`.

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
| `loqed/<id>/event` | no | JSON lock event (incl. `command_failed`) |
| `loqed/<id>/command` | must not be retained | `LOCK`, `UNLOCK` or `OPEN` |

Retained messages on the command topic are ignored.

## Releasing

1. Tag `v<version>` and push the tag. CI tests, then pushes
   `ghcr.io/t3hk0d3/loqed-mqtt` (amd64, arm64, arm/v7) and
   `ghcr.io/t3hk0d3/loqed-mqtt-addon` (amd64, arm64). Prerelease tags
   (`v1.2.0-rc1`) do not move `latest`.
2. First release only: make both GHCR packages public (package settings →
   change visibility), otherwise the Supervisor and `docker pull` fail.
3. After the images exist, bump `version` in `addon/config.yaml` to the same
   version and push. (Bumping first would make add-on updates fail.)

## Development

    go test -race ./...
    go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.14.0 run ./...
    python3 testdata/gen_vectors.py   # regenerate signing golden vectors
```

`.github/workflows/ci.yml`:

```yaml
name: ci
on:
  push:
    branches: [main]
  pull_request:
permissions:
  contents: read
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v5
      - uses: actions/setup-go@v6
        with:
          go-version-file: go.mod
      - run: test -z "$(gofmt -l .)"
      - run: go vet ./...
      - run: go test -race ./...
      - uses: golangci/golangci-lint-action@v8
        with:
          version: v2.14.0
  addon:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v5
      - uses: frenck/action-addon-linter@v2
        with:
          path: ./addon
  image:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v5
      - uses: docker/setup-qemu-action@v3
      - uses: docker/setup-buildx-action@v3
      - uses: docker/build-push-action@v6
        with:
          context: .
          target: standalone
          platforms: linux/amd64,linux/arm64,linux/arm/v7
          push: false
      - uses: docker/build-push-action@v6
        with:
          context: .
          target: addon
          platforms: linux/amd64,linux/arm64
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
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v5
      - uses: actions/setup-go@v6
        with:
          go-version-file: go.mod
      - run: go test -race ./...
  image:
    needs: test
    runs-on: ubuntu-latest
    permissions:
      contents: read
      packages: write
    strategy:
      matrix:
        include:
          - target: standalone
            image: ghcr.io/t3hk0d3/loqed-mqtt
            platforms: linux/amd64,linux/arm64,linux/arm/v7
          - target: addon
            image: ghcr.io/t3hk0d3/loqed-mqtt-addon
            platforms: linux/amd64,linux/arm64
    steps:
      - uses: actions/checkout@v5
      - uses: docker/setup-qemu-action@v3
      - uses: docker/setup-buildx-action@v3
      - uses: docker/login-action@v3
        with:
          registry: ghcr.io
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}
      - id: meta
        uses: docker/metadata-action@v5
        with:
          images: ${{ matrix.image }}
          # {{version}} drops the "v"; latest only for non-prerelease tags.
          tags: type=semver,pattern={{version}}
          flavor: latest=auto
      - uses: docker/build-push-action@v6
        with:
          context: .
          target: ${{ matrix.target }}
          platforms: ${{ matrix.platforms }}
          push: true
          build-args: VERSION=${{ steps.meta.outputs.version }}
          tags: ${{ steps.meta.outputs.tags }}
          labels: ${{ steps.meta.outputs.labels }}
```

Action versions are the latest majors known when this plan was written (checkout v5, setup-go v6, golangci-lint-action v8, docker/* v3–v6, metadata-action v5, action-addon-linter v2); bump any that GitHub reports as deprecated.

- [ ] **Step 4: Verify**

Run:

```bash
gofmt -l . && go vet ./... && go test -race ./...
go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.14.0 run ./...
python3 -c "import yaml,sys; [yaml.safe_load(open(f)) for f in sys.argv[1:]]" addon/config.yaml addon/translations/en.yaml repository.yaml docker-compose.yml .github/workflows/ci.yml .github/workflows/release.yml
```

Expected: tests pass; `0 issues.`; YAML files parse (if PyYAML is missing, `pip install pyyaml` in a venv or skip this line).

- [ ] **Step 5: Commit**

```bash
git add Dockerfile .dockerignore docker-compose.yml addon repository.yaml README.md internal/config/addon_test.go .github
git commit -m "Add Docker images, Home Assistant add-on, CI and docs

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 16: Real-hardware verification (gate for v1.0.0)

> Runs after `2026-10-06-loqed-mqtt-rev-2-1.md` (spec rev 2.1). V1 and V3–V7 already have recorded outcomes in spec 2.5; V2, V8 and V9 remain.

**Files:**
- Create: `docs/verification.md`

This task needs a real LOQED lock + bridge, a LOQED account and (for V8) a Home Assistant OS install. It records outcomes; `v1.0.0` is not tagged until every item has one. Pre-release tags (`v0.x`) may be published before it to test the add-on (V8).

- [ ] **Step 1: Create the checklist**

`docs/verification.md`:

```markdown
# Verification against real hardware (spec §2.4)

Date / gateway version / bridge firmware:

| # | Check | How | Outcome |
|---|---|---|---|
| V1 | `/api/locks/` still returns `bridge_ip`, `local_id`, `key_secret`, `bridge_key` | first start with an empty cache; `locks.json` has them | |
| V2 | which endpoints count toward 12 per 12 h; do commands count | after a block, note which calls preceded it (support ticket if unclear) | |
| V3 | create-webhook hash (flags u32 BE) accepted by current firmware | gateway registers its webhook without errors | |
| V4 | bridge HTTP port is 80 | TCP probe succeeds; `curl http://<bridge>/status` | |
| V5 | unit of `wifi_strength` / `ble_strength` (% or dBm) | compare `/status` and webhook values with the app | |
| V6 | do cloud webhooks carry a signature header | log request header names once (not values) on `/cloud/` | |
| V7 | portal login + token mint end to end (CSRF meta vs cookie, `remember`, 2FA/SSO accounts) | start with e-mail/password only | |
| V8 | HA OS: add-on installs from the prebuilt image, options form renders `lock_settings`, defaults validate, `core-mosquitto` (or the 127.0.0.1 fallback) connects | install from the repository on HAOS | |
```

- [ ] **Step 2: Run each check and record the outcome**

Decision points:
- V2: if commands are counted separately from reads, nothing changes (they are already recorded); if they do not count, remove `Budget.Record` from `CloudHub.command` in a follow-up.
- V5: set `unit_of_measurement` (and `device_class: signal_strength` for dBm) on the two signal sensors in `internal/hass/discovery.go` and regenerate the golden file.
- V6: if a signature header exists and its scheme can be determined, add verification to `webhook.cloudWebhook` in addition to the path secret.
- V7: if the portal rejects the flow, document `cloud_token` as the only supported method in `addon/DOCS.md`.
- V8: if `lock_settings` does not render in the options form, document YAML-mode editing (already mentioned) or flatten the schema.

- [ ] **Step 3: Commit**

```bash
git add docs/verification.md
git commit -m "Record real-hardware verification results

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Spec coverage

| Spec section | Task |
|---|---|
| §2.4 V1–V8 verification | 16 (gates v1.0.0) |
| §4 library | library plan |
| §5.1 configuration (sources, forms, JSON options, validation, warnings) | 1; warnings for unmatched names/settings in 14 |
| §5.2 cloud authentication (install-id names, create-before-revoke, e-mail scoped, persisted hourly limit) | 2, 3, 14 |
| §5.3 credential cache, merge, empty-list guard, refresh rules 1–6, backoff, pinned skips | 2, 8, 9, 10, 14 |
| §5.4 startup (published_ids cleanup, clean stop) | 14 |
| §5.5 supervisor: modes, separate failure counters, webhook retry, GoTo/MOTOR_STALL confirms, freshness, commands and fallback rules, deadlines, `command_failed` | 9, 10, 11 |
| §5.6 cloud budget (priorities, persistence, command recording, notBefore) | 7, 10, 14 |
| §5.7 state + event mapping (event_type-derived state), key names, enrichment with key match | 4, 9, 12 |
| §6 MQTT + discovery (retained-command guard, ordered republish, removal incl. runtime) | 5, 6, 12, 14 |
| §7 webhook listener (health grace, timeouts, container warning) | 13, 14 |
| §8 logging (JSON format, redacted URLs, rate-limited repeats via `Supervisor.warn`) | 6, 9, 14 |
| §9 packaging (two targets, prebuilt add-on, release order) | 15 |
| §10 testing (real-hub tests, e2e, CI lint/gofmt/add-on lint) | every task; real hub in 12; end-to-end in 14 |

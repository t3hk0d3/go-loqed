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
		!cfg.EventDedupEnabled || cfg.EventDedupWindow.D() != 10*time.Second || cfg.CloudBudget != 10 || cfg.Webhook.Listen != ":8099" || cfg.MQTT.ClientID != "loqed-mqtt" || cfg.MQTT.BaseTopic != "loqed" ||
		!cfg.HomeAssistant.Enabled || cfg.HomeAssistant.DiscoveryPrefix != "homeassistant" || cfg.LogLevel != "info" {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
}

func TestPrecedenceOptionsThenYAMLThenEnv(t *testing.T) {
	opts := write(t, "options.json", `{"cloud_token":"from-options","mqtt":{"base_topic":"opt","url":"tcp://opt:1883"},"cloud_budget":5}`)
	yml := write(t, "config.yaml", "mqtt:\n  base_topic: yaml\nliveness_interval: 30s\nevent_dedup_window: 5s\n")
	cfg, err := config.Load(config.Sources{OptionsFile: opts, ConfigFile: yml, Environ: []string{
		"LOQED_MQTT__BASE_TOPIC=env", "LOQED_CLOUD_BUDGET=7", "LOQED_EVENT_DEDUP_ENABLED=false", "LOQED_HOMEASSISTANT__ENABLED=false", "PATH=/bin",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.CloudToken != "from-options" || cfg.MQTT.URL != "tcp://opt:1883" || cfg.MQTT.BaseTopic != "env" ||
		cfg.LivenessInterval.D() != 30*time.Second || cfg.EventDedupWindow.D() != 5*time.Second || cfg.EventDedupEnabled || cfg.CloudBudget != 7 || cfg.HomeAssistant.Enabled {
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
	bad.EventDedupWindow = config.Duration(31 * time.Second)
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
	for _, want := range []string{"cloud_password", "cloud_budget", "liveness_interval", "event_dedup_window", "public_url", "private_url must not have a path", "cloud_secret", "base_topic", "log_level", "log_format", "bridge_ip", "bridge_key"} {
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

func TestUnknownKeysInsideLockSettingsFail(t *testing.T) {
	cases := map[string]config.Sources{
		"yaml map":      {ConfigFile: write(t, "c.yaml", "lock_settings: {Front: {bridge_ipp: 1.2.3.4}}\n")},
		"options list":  {OptionsFile: write(t, "options.json", `{"lock_settings":[{"lock":"Front","bridge_ipp":"1.2.3.4"}]}`)},
		"env":           {Environ: []string{`LOQED_LOCK_SETTINGS={"Front":{"bridge_ipp":"1.2.3.4"}}`}},
		"key_names obj": {ConfigFile: write(t, "k.yaml", "lock_settings: {Front: {key_names: [{id: 1, nam: A}]}}\n")},
	}
	for name, src := range cases {
		_, err := config.Load(src)
		if err == nil || !strings.Contains(err.Error(), "not found") {
			t.Errorf("%s: expected unknown-field error, got %v", name, err)
		}
	}
}

func TestKeyNamesEntryWithoutIDFails(t *testing.T) {
	src := config.Sources{ConfigFile: write(t, "c.yaml", "lock_settings: {Front: {key_names: [{name: Alice}]}}\n")}
	if _, err := config.Load(src); err == nil || !strings.Contains(err.Error(), "id") {
		t.Fatalf("expected error about id, got %v", err)
	}
}

func TestMQTTCloudWebhooksDefaultsOff(t *testing.T) {
	cfg, err := config.Load(config.Sources{OptionsFile: "/nonexistent/options.json"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MQTT.CloudWebhooks {
		t.Fatal("mqtt.cloud_webhooks must default to false")
	}
}

func TestMQTTCloudWebhooksFromEachSource(t *testing.T) {
	for name, src := range map[string]config.Sources{
		"options.json": {OptionsFile: write(t, "options.json", `{"mqtt":{"cloud_webhooks":true}}`)},
		"YAML":         {ConfigFile: write(t, "config.yaml", "mqtt:\n  cloud_webhooks: true\n")},
		"env":          {Environ: []string{"LOQED_MQTT__CLOUD_WEBHOOKS=true"}},
	} {
		t.Run(name, func(t *testing.T) {
			if src.OptionsFile == "" {
				src.OptionsFile = "/nonexistent/options.json"
			}
			cfg, err := config.Load(src)
			if err != nil {
				t.Fatal(err)
			}
			if !cfg.MQTT.CloudWebhooks {
				t.Fatalf("%s: cloud_webhooks not set", name)
			}
		})
	}
}

func TestMQTTCloudWebhooksPrecedence(t *testing.T) {
	cfg, err := config.Load(config.Sources{
		OptionsFile: write(t, "options.json", `{"mqtt":{"cloud_webhooks":true}}`),
		ConfigFile:  write(t, "config.yaml", "mqtt:\n  cloud_webhooks: false\n"),
		Environ:     []string{"LOQED_MQTT__CLOUD_WEBHOOKS=true"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.MQTT.CloudWebhooks {
		t.Fatal("env must override YAML")
	}
	cfg, err = config.Load(config.Sources{
		OptionsFile: write(t, "options.json", `{"mqtt":{"cloud_webhooks":true}}`),
		ConfigFile:  write(t, "config.yaml", "mqtt:\n  cloud_webhooks: false\n"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MQTT.CloudWebhooks {
		t.Fatal("YAML must override options.json")
	}
}

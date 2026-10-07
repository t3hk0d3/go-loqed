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
	case "enabled", "event_dedup_enabled", "cloud_webhooks":
		return true
	case "cloud_budget":
		return 10
	case "cache_max_age", "reconcile_interval", "liveness_interval", "event_dedup_window":
		return "1h"
	case "log_level":
		return "info"
	case "log_format":
		return "json"
	default:
		return ""
	}
}

// Home Assistant hides options without a default under "Show unused
// optional configuration options"; the cloud credentials are the first
// thing a user must set, so they need an (empty) default.
func TestAddonShowsCloudCredentialsByDefault(t *testing.T) {
	raw, err := os.ReadFile("../../addon/config.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var addon struct {
		Options map[string]any `yaml:"options"`
	}
	if err := yaml.Unmarshal(raw, &addon); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"cloud_token", "cloud_email", "cloud_password"} {
		if v, ok := addon.Options[k]; !ok || v != "" {
			t.Errorf("options.%s = %v (present %v), want an empty default", k, v, ok)
		}
	}
}

package config_test

import (
	"bytes"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/t3hk0d3/go-loqed/loqed-mqtt/internal/config"
)

const (
	fakeToken        = "FAKE-CLOUD-TOKEN"
	fakePassword     = "FAKE-CLOUD-PASSWORD"
	fakeMQTTPass     = "FAKE-MQTT-PASSWORD"
	fakeHookSecret   = "FAKE-CLOUD-WEBHOOK-SECRET"
	fakeBridgeKey    = "RkFLRS1CUklER0UtS0VZ"
	fakeKeySecret    = "RkFLRS1LRVktU0VDUkVU"
	fakeEnvKeySecret = "RkFLRS1FTlYtS0VZLVNFQ1JFVA=="
)

var fakeConfigSecrets = []string{fakeToken, fakePassword, fakeMQTTPass, fakeHookSecret, fakeBridgeKey, fakeKeySecret, fakeEnvKeySecret}

func loadSecretConfig(t *testing.T) config.Config {
	t.Helper()
	opts := write(t, "options.json", `{"cloud_email":"me@example.com","cloud_password":"`+fakePassword+`",`+
		`"lock_settings":[{"lock":"front","bridge_key":"`+fakeBridgeKey+`","key_secret":"`+fakeKeySecret+`"}]}`)
	yml := write(t, "config.yaml", "mqtt:\n  password: "+fakeMQTTPass+"\nwebhook:\n  cloud_secret: "+fakeHookSecret+"\n")
	cfg, err := config.Load(config.Sources{OptionsFile: opts, ConfigFile: yml, Environ: []string{
		"LOQED_CLOUD_TOKEN=" + fakeToken,
		`LOQED_LOCK_SETTINGS={"back": {"key_secret": "` + fakeEnvKeySecret + `"}}`,
	}})
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func TestLoadKeepsTheRealSecrets(t *testing.T) {
	cfg := loadSecretConfig(t)
	if cfg.CloudToken.Reveal() != fakeToken || cfg.CloudPassword.Reveal() != fakePassword ||
		cfg.MQTT.Password.Reveal() != fakeMQTTPass || cfg.Webhook.CloudSecret.Reveal() != fakeHookSecret ||
		cfg.LockSettings["front"].BridgeKey.Reveal() != fakeBridgeKey || cfg.LockSettings["front"].KeySecret.Reveal() != fakeKeySecret ||
		cfg.LockSettings["back"].KeySecret.Reveal() != fakeEnvKeySecret {
		t.Fatal("loaded secrets differ from the sources")
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestPrintingTheConfigDoesNotLeakSecrets(t *testing.T) {
	cfg := loadSecretConfig(t)
	values := []any{cfg, &cfg, cfg.LockSettings, cfg.LockSettings["front"], cfg.MQTT, cfg.Webhook}
	var outs []string
	for _, verb := range []string{"%v", "%+v", "%#v", "%s"} {
		for _, v := range values {
			outs = append(outs, fmt.Sprintf(verb, v))
		}
	}
	for _, h := range []func(*bytes.Buffer) slog.Handler{
		func(b *bytes.Buffer) slog.Handler { return slog.NewTextHandler(b, nil) },
		func(b *bytes.Buffer) slog.Handler { return slog.NewJSONHandler(b, nil) },
	} {
		for _, v := range values {
			var buf bytes.Buffer
			slog.New(h(&buf)).Info("config", "v", v)
			outs = append(outs, buf.String())
		}
	}
	for _, out := range outs {
		for _, s := range fakeConfigSecrets {
			if strings.Contains(out, s) {
				t.Fatalf("secret leaked: %s", out)
			}
		}
	}
}

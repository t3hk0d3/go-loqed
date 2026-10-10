package mqtt_test

import (
	"bytes"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/t3hk0d3/go-loqed/loqed-mqtt/internal/mqtt"
)

func TestPrintingTheClientConfigDoesNotLeakThePassword(t *testing.T) {
	const password = "FAKE-MQTT-PASSWORD"
	cfg := mqtt.ClientConfig{URL: "tcp://broker:1883", Username: "loqed", Password: password}
	var outs []string
	for _, verb := range []string{"%v", "%+v", "%#v", "%s"} {
		outs = append(outs, fmt.Sprintf(verb, cfg), fmt.Sprintf(verb, &cfg))
	}
	var buf bytes.Buffer
	slog.New(slog.NewTextHandler(&buf, nil)).Info("mqtt", "v", cfg)
	outs = append(outs, buf.String())
	for _, out := range outs {
		if strings.Contains(out, password) {
			t.Fatalf("password leaked: %s", out)
		}
	}
}

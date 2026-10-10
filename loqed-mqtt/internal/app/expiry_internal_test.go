package app

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/t3hk0d3/go-loqed/loqed-mqtt/internal/config"
	"github.com/t3hk0d3/go-loqed/loqed-mqtt/internal/gateway"
	"github.com/t3hk0d3/go-loqed/loqed-mqtt/internal/mqtt"
)

func TestWatchTokenExpiryChecksAtOnceAndPeriodically(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var checks, remints atomic.Int32
	check := func(context.Context) (bool, error) {
		n := checks.Add(1)
		return n == 2, nil // the second check replaces the token
	}
	done := make(chan struct{})
	go func() {
		watchTokenExpiry(ctx, check, func(context.Context) { remints.Add(1) }, 20*time.Millisecond, slog.New(slog.DiscardHandler))
		close(done)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for checks.Load() < 3 {
		if time.Now().After(deadline) {
			t.Fatalf("checks %d", checks.Load())
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done
	if remints.Load() != 1 {
		t.Fatalf("remints %d, want 1 (only after a replacement)", remints.Load())
	}
}

func TestCloudWebhooksConfiguredFromEitherSource(t *testing.T) {
	for _, c := range []struct {
		publicURL string
		overMQTT  bool
		want      bool
	}{
		{"", false, false},
		{"https://loqed.example.com", false, true},
		{"", true, true},
		{"https://loqed.example.com", true, true},
	} {
		cfg := config.Defaults()
		cfg.Webhook.PublicURL, cfg.MQTT.CloudWebhooks = c.publicURL, c.overMQTT
		if got := cloudWebhooksConfigured(cfg); got != c.want {
			t.Errorf("public_url %q, mqtt.cloud_webhooks %v: got %v", c.publicURL, c.overMQTT, got)
		}
	}
}

func TestForwardCloudWebhooksReturnsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	mq := mqtt.NewClient(mqtt.ClientConfig{URL: "tcp://127.0.0.1:1", ClientID: "x"}, slog.New(slog.DiscardHandler))
	done := make(chan struct{})
	go func() {
		forwardCloudWebhooks(ctx, mq, gateway.NewManager(nil), slog.New(slog.DiscardHandler))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("forwardCloudWebhooks did not return after cancel")
	}
}

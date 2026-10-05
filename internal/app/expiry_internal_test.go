package app

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"
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

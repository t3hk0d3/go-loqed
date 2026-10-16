package app

import (
	"strings"
	"testing"
	"time"
)

func TestDefaultClockCarriesMonotonicReading(t *testing.T) {
	if s := clockOr(nil)().String(); !strings.Contains(s, "m=") {
		t.Fatalf("default clock value %q has no monotonic reading; deadlines would follow wall-clock steps", s)
	}
}

func TestInjectedClockIsUsed(t *testing.T) {
	fixed := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	if got := clockOr(func() time.Time { return fixed })(); !got.Equal(fixed) {
		t.Fatalf("clockOr ignored the injected clock: %v", got)
	}
}

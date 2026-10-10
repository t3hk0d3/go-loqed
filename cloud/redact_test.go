package cloud_test

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/t3hk0d3/go-loqed/cloud"
)

// Secrets of the locksJSON fixture: key_secret, bridge_key, backend_key.
var fixtureSecrets = []string{"SGFsbG8gd2VyZWxk", "Ym9uam91ciBtb25kZQ==", "aGVsbG8gd29ybGQ="}

func listFixtureLocks(t *testing.T) []cloud.Lock {
	t.Helper()
	c := newServer(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(locksJSON)) })
	locks, err := c.ListLocks(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return locks
}

func TestListLocksDecodesTheRealSecrets(t *testing.T) {
	a := listFixtureLocks(t)[0]
	if a.KeySecret.Reveal() != fixtureSecrets[0] || a.BridgeKey.Reveal() != fixtureSecrets[1] || a.BackendKey.Reveal() != fixtureSecrets[2] {
		t.Fatal("decoded lock secrets differ from the fixture")
	}
}

func TestPrintingLocksDoesNotLeakSecrets(t *testing.T) {
	locks := listFixtureLocks(t)
	type holder struct{ Lock cloud.Lock }
	values := []any{locks[0], &locks[0], locks, holder{locks[0]}}
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
			slog.New(h(&buf)).Info("locks", "v", v)
			outs = append(outs, buf.String())
		}
	}
	for _, out := range outs {
		for _, s := range fixtureSecrets {
			if strings.Contains(out, s) {
				t.Fatalf("secret leaked: %s", out)
			}
		}
		if !strings.Contains(out, "MyLock") {
			t.Fatalf("non-secret fields missing: %s", out)
		}
	}
}

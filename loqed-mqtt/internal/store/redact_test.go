package store_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/t3hk0d3/go-loqed/loqed-mqtt/internal/store"
)

// Secrets in testdata/locks.json.
var fixtureSecrets = []string{
	"FAKE-MINTED-TOKEN", "FAKE-CLOUD-WEBHOOK-SECRET",
	"RkFLRS1LRVktU0VDUkVU", "RkFLRS1CUklER0UtS0VZ", "RkFLRS1CQUNLRU5ELUtFWQ==",
}

func openFixture(t *testing.T) (*store.Store, string) {
	t.Helper()
	b, err := os.ReadFile("testdata/locks.json")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "locks.json")
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	st, status, err := store.Open(path)
	if err != nil || status != store.StatusLoaded {
		t.Fatalf("status %v err %v", status, err)
	}
	return st, path
}

func TestOpenReadsTheRealSecretsFromTheCacheFile(t *testing.T) {
	st, _ := openFixture(t)
	c := st.Snapshot()
	r := c.Locks[0]
	if c.Minted == nil || c.Minted.Value.Reveal() != "FAKE-MINTED-TOKEN" || c.CloudSecret.Reveal() != "FAKE-CLOUD-WEBHOOK-SECRET" ||
		r.KeySecret.Reveal() != "RkFLRS1LRVktU0VDUkVU" || r.BridgeKey.Reveal() != "RkFLRS1CUklER0UtS0VZ" ||
		r.BackendKey.Reveal() != "RkFLRS1CQUNLRU5ELUtFWQ==" || r.CloudWebhookID != "1234" || r.Name != "Front door" {
		t.Fatal("loaded cache differs from testdata/locks.json")
	}
}

func TestWritingTheCacheKeepsTheFileFormatAndSecrets(t *testing.T) {
	st, path := openFixture(t)
	if err := st.CheckWritable(); err != nil {
		t.Fatal(err)
	}
	want, got := decodeFile(t, "testdata/locks.json"), decodeFile(t, path)
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("rewritten cache differs from the original:\nwant %v\ngot  %v", want, got)
	}
	again, status, err := store.Open(path)
	if err != nil || status != store.StatusLoaded {
		t.Fatalf("status %v err %v", status, err)
	}
	if !reflect.DeepEqual(st.Snapshot(), again.Snapshot()) {
		t.Fatal("reopened cache differs")
	}
}

func decodeFile(t *testing.T, path string) map[string]any {
	t.Helper()
	b, err := os.ReadFile(path) //nolint:gosec // test file
	if err != nil {
		t.Fatal(err)
	}
	var v map[string]any
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatal(err)
	}
	return v
}

func TestPrintingTheCacheDoesNotLeakSecrets(t *testing.T) {
	st, _ := openFixture(t)
	c := st.Snapshot()
	values := []any{c, &c, c.Locks, c.Locks[0], c.Minted}
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
			slog.New(h(&buf)).Info("cache", "v", v)
			outs = append(outs, buf.String())
		}
	}
	for _, out := range outs {
		for _, s := range fixtureSecrets {
			if strings.Contains(out, s) {
				t.Fatalf("secret leaked: %s", out)
			}
		}
	}
}

package auth_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/t3hk0d3/go-loqed/loqed-mqtt/internal/auth"
	"github.com/t3hk0d3/go-loqed/loqed-mqtt/internal/store"
)

// jwt builds an unsigned-looking token; expiry checks never verify signatures.
func jwt(payload string) string {
	enc := base64.RawURLEncoding.EncodeToString
	return enc([]byte(`{"typ":"JWT","alg":"RS256"}`)) + "." + enc([]byte(payload)) + ".c2lnbmF0dXJl"
}

func TestTokenExpiry(t *testing.T) {
	exp := time.Date(2027, 4, 5, 19, 15, 26, 0, time.UTC)
	got, ok := auth.TokenExpiry(jwt(`{"aud":"1","exp":1806952526,"sub":"42"}`))
	if !ok || !got.Equal(exp) || got.Location() != time.UTC {
		t.Fatalf("got %v %v", got, ok)
	}
	for name, tok := range map[string]string{
		"not a jwt":      "plain-token",
		"bad base64":     "a.!!!.c",
		"bad json":       jwt(`{`),
		"no exp":         jwt(`{"sub":"42"}`),
		"string exp":     jwt(`{"exp":"soon"}`),
		"two parts only": "a.b",
	} {
		if _, ok := auth.TokenExpiry(tok); ok {
			t.Errorf("%s: expected no expiry", name)
		}
	}
}

type clock struct{ now time.Time }

func (c *clock) Now() time.Time { return c.now }

func expiring(c *clock, in time.Duration) string {
	return jwt(`{"exp":` + itoa(c.now.Add(in).Unix()) + `}`)
}

func itoa(v int64) string { return strconv.FormatInt(v, 10) }

func logBuffer() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	return slog.New(slog.NewTextHandler(&buf, nil)), &buf
}

func TestCheckExpiryFarAwayDoesNothing(t *testing.T) {
	c := &clock{now: time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC)}
	log, buf := logBuffer()
	m := &fakeMinter{}
	r := auth.NewResolver(expiring(c, 30*24*time.Hour), "", m, newStore(t), c.Now, log)
	reminted, err := r.CheckExpiry(context.Background())
	if reminted || err != nil || m.calls != 0 || buf.Len() != 0 {
		t.Fatalf("reminted=%v err=%v calls=%d log=%q", reminted, err, m.calls, buf)
	}
}

func TestCheckExpiryConfiguredTokenWarnsDaily(t *testing.T) {
	c := &clock{now: time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC)}
	log, buf := logBuffer()
	m := &fakeMinter{}
	tok := expiring(c, 10*24*time.Hour)
	r := auth.NewResolver(tok, "", m, newStore(t), c.Now, log)
	for range 3 {
		if reminted, err := r.CheckExpiry(context.Background()); reminted || err != nil {
			t.Fatalf("reminted=%v err=%v", reminted, err)
		}
		c.now = c.now.Add(time.Hour)
	}
	if n := strings.Count(buf.String(), "level=WARN"); n != 1 {
		t.Fatalf("want 1 warning within a day, got %d: %s", n, buf)
	}
	c.now = c.now.Add(24 * time.Hour)
	_, _ = r.CheckExpiry(context.Background())
	if n := strings.Count(buf.String(), "level=WARN"); n != 2 {
		t.Fatalf("want a second warning the next day, got %d", n)
	}
	if m.calls != 0 || strings.Contains(buf.String(), tok) {
		t.Fatalf("minted %d times or logged the token", m.calls)
	}
	if !strings.Contains(buf.String(), "2026-10-16") {
		t.Fatalf("warning does not name the expiry: %s", buf)
	}
}

func TestCheckExpiryRemintsMintedToken(t *testing.T) {
	c := &clock{now: time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC)}
	st := newStore(t)
	_ = st.Update(func(cc *store.Cache) {
		cc.Minted = &store.MintedToken{ID: "x", Value: expiring(c, 13*24*time.Hour), EmailSHA256: store.EmailHash("me@example.com")}
	})
	m := &fakeMinter{}
	r := auth.NewResolver("", "me@example.com", m, st, c.Now, slog.New(slog.DiscardHandler))
	reminted, err := r.CheckExpiry(context.Background())
	if !reminted || err != nil || m.calls != 1 {
		t.Fatalf("reminted=%v err=%v calls=%d", reminted, err, m.calls)
	}
	if got := st.Snapshot().Minted.Value; got != "minted-1" {
		t.Fatalf("new token not stored: %q", got)
	}
}

func TestCheckExpiryMintFailureKeepsOldToken(t *testing.T) {
	c := &clock{now: time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC)}
	st := newStore(t)
	old := expiring(c, 13*24*time.Hour)
	_ = st.Update(func(cc *store.Cache) {
		cc.Minted = &store.MintedToken{ID: "x", Value: old, EmailSHA256: store.EmailHash("me@example.com")}
	})
	log, buf := logBuffer()
	m := &fakeMinter{err: errors.New("portal down")}
	r := auth.NewResolver("", "me@example.com", m, st, c.Now, log)
	if reminted, err := r.CheckExpiry(context.Background()); reminted || err == nil {
		t.Fatalf("reminted=%v err=%v", reminted, err)
	}
	if tok, _ := r.Token(context.Background()); tok != old {
		t.Fatal("the old token must stay in use")
	}
	if !strings.Contains(buf.String(), "level=WARN") {
		t.Fatalf("no warning: %s", buf)
	}
}

func TestExpiryOfCurrentToken(t *testing.T) {
	c := &clock{now: time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC)}
	r := auth.NewResolver(expiring(c, time.Hour), "", nil, newStore(t), c.Now, slog.New(slog.DiscardHandler))
	if got, ok := r.Expiry(); !ok || !got.Equal(c.now.Add(time.Hour)) {
		t.Fatalf("got %v %v", got, ok)
	}
	if _, ok := auth.NewResolver("", "", nil, newStore(t), c.Now, slog.New(slog.DiscardHandler)).Expiry(); ok {
		t.Fatal("no token: expiry must be unknown")
	}
}

func TestKeyDeletedConfiguredTokenExplains(t *testing.T) {
	r := auth.NewResolver("configured", "", &fakeMinter{}, newStore(t), time.Now, slog.New(slog.DiscardHandler))
	_, err := r.KeyDeleted(context.Background(), "configured")
	if err == nil || !strings.Contains(err.Error(), "deleted in the LOQED app") {
		t.Fatalf("got %v", err)
	}
}

func TestKeyDeletedRemintsMintedToken(t *testing.T) {
	st := newStore(t)
	_ = st.Update(func(c *store.Cache) {
		c.Minted = &store.MintedToken{ID: "x", Value: "dead-key", EmailSHA256: store.EmailHash("me@example.com")}
	})
	m := &fakeMinter{}
	r := auth.NewResolver("", "me@example.com", m, st, time.Now, slog.New(slog.DiscardHandler))
	tok, err := r.KeyDeleted(context.Background(), "dead-key")
	if err != nil || tok != "minted-1" || m.calls != 1 {
		t.Fatalf("%q %v calls=%d", tok, err, m.calls)
	}
}

// blockingMinter mints only when released, like a slow portal.
type blockingMinter struct{ entered, release chan struct{} }

func (m *blockingMinter) Mint(ctx context.Context) (store.MintedToken, error) {
	close(m.entered)
	<-m.release
	return store.MintedToken{ID: "id", Value: "minted"}, nil
}

// Expiry runs on every supervisor's publish; it must not wait for a mint
// that holds the resolver for up to the portal timeout.
func TestExpiryDoesNotWaitForAMint(t *testing.T) {
	m := &blockingMinter{entered: make(chan struct{}), release: make(chan struct{})}
	r := auth.NewResolver("", "me@example.com", m, newStore(t), time.Now, discard)
	minted := make(chan struct{})
	go func() {
		defer close(minted)
		_, _ = r.Token(context.Background())
	}()
	<-m.entered
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.Expiry()
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Error("Expiry waited for the mint")
	}
	close(m.release)
	<-minted
	<-done
}

package auth_test

import (
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/cloud/portal"
	"github.com/t3hk0d3/go-loqed/loqed-mqtt/internal/auth"
	"github.com/t3hk0d3/go-loqed/loqed-mqtt/internal/store"
)

type fakeMinter struct {
	calls int
	err   error
}

func (m *fakeMinter) Mint(context.Context) (store.MintedToken, error) {
	m.calls++
	if m.err != nil {
		return store.MintedToken{}, m.err
	}
	return store.MintedToken{ID: "id", Value: "minted-" + string(rune('0'+m.calls))}, nil
}

func newStore(t *testing.T) *store.Store {
	t.Helper()
	st, _, err := store.Open(filepath.Join(t.TempDir(), "locks.json"))
	if err != nil {
		t.Fatal(err)
	}
	return st
}

var discard = slog.New(slog.DiscardHandler)

func TestConfiguredTokenWins(t *testing.T) {
	m := &fakeMinter{}
	r := auth.NewResolver("configured", "", m, newStore(t), time.Now, discard)
	tok, err := r.Token(context.Background())
	if err != nil || tok != "configured" || m.calls != 0 {
		t.Fatalf("%q %v calls=%d", tok, err, m.calls)
	}
	if _, err := r.Invalidate(context.Background(), "configured"); !errors.Is(err, loqed.ErrUnauthorized) {
		t.Fatalf("got %v", err)
	}
}

func TestCachedMintedTokenIsReused(t *testing.T) {
	st := newStore(t)
	_ = st.Update(func(c *store.Cache) {
		c.Minted = &store.MintedToken{ID: "x", Value: "cached", EmailSHA256: store.EmailHash("me@example.com")}
	})
	m := &fakeMinter{}
	tok, err := auth.NewResolver("", "me@example.com", m, st, time.Now, discard).Token(context.Background())
	if err != nil || tok != "cached" || m.calls != 0 {
		t.Fatalf("%q %v calls=%d", tok, err, m.calls)
	}
}

func TestMintsAndPersists(t *testing.T) {
	st := newStore(t)
	m := &fakeMinter{}
	tok, err := auth.NewResolver("", "me@example.com", m, st, time.Now, discard).Token(context.Background())
	if err != nil || tok != "minted-1" {
		t.Fatalf("%q %v", tok, err)
	}
	if got := st.Snapshot().Minted; got == nil || got.Value != "minted-1" {
		t.Fatalf("not persisted: %+v", got)
	}
}

func TestNoCredentials(t *testing.T) {
	if _, err := auth.NewResolver("", "", nil, newStore(t), time.Now, discard).Token(context.Background()); !errors.Is(err, auth.ErrNoToken) {
		t.Fatalf("got %v", err)
	}
}

func TestInvalidateRemintsAtMostHourly(t *testing.T) {
	now := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	m := &fakeMinter{}
	r := auth.NewResolver("", "me@example.com", m, newStore(t), func() time.Time { return now }, discard)
	ctx := context.Background()
	first, _ := r.Token(ctx)
	second, err := r.Invalidate(ctx, first)
	if !errors.Is(err, loqed.ErrUnauthorized) || second != "" || m.calls != 1 {
		t.Fatalf("re-mint within the hour must be refused: %q %v calls=%d", second, err, m.calls)
	}
	now = now.Add(time.Hour)
	second, err = r.Invalidate(ctx, first)
	if err != nil || second != "minted-2" {
		t.Fatalf("%q %v", second, err)
	}
	// Someone already replaced the rejected token: return the new one without minting.
	third, err := r.Invalidate(ctx, first)
	if err != nil || third != "minted-2" || m.calls != 2 {
		t.Fatalf("%q %v calls=%d", third, err, m.calls)
	}
}

type fakeSession struct {
	tokens    []portal.TokenInfo
	revoked   []string
	created   []string
	order     []string
	createErr error
	logout    bool
}

func (s *fakeSession) ListTokens(context.Context) ([]portal.TokenInfo, error) {
	s.order = append(s.order, "list")
	return s.tokens, nil
}
func (s *fakeSession) CreateToken(_ context.Context, name string) (portal.Token, error) {
	s.order = append(s.order, "create")
	if s.createErr != nil {
		return portal.Token{}, s.createErr
	}
	s.created = append(s.created, name)
	tok := portal.Token{ID: "new", Name: name, Value: "pat"}
	s.tokens = append(s.tokens, portal.TokenInfo{ID: tok.ID, Name: name})
	return tok, nil
}
func (s *fakeSession) RevokeToken(_ context.Context, id string) error {
	s.order = append(s.order, "revoke")
	s.revoked = append(s.revoked, id)
	return nil
}
func (s *fakeSession) Logout(context.Context) error { s.logout = true; return nil }

func TestPortalMinterCreatesBeforeRevoking(t *testing.T) {
	name := auth.TokenName("1a2b3c4d")
	sess := &fakeSession{tokens: []portal.TokenInfo{{ID: "old", Name: name}, {ID: "keep", Name: "HA"}}}
	m := &auth.PortalMinter{
		Login: func(_ context.Context, email, password string) (auth.PortalSession, error) {
			if email != "me" || password != "pw" {
				t.Fatalf("credentials %q %q", email, password)
			}
			return sess, nil
		},
		Email: "me", Password: "pw", TokenName: name,
	}
	tok, err := m.Mint(context.Background())
	if err != nil || tok.ID != "new" || tok.Value != "pat" {
		t.Fatalf("%+v %v", tok, err)
	}
	if len(sess.revoked) != 1 || sess.revoked[0] != "old" || sess.created[0] != "loqed-mqtt 1a2b3c4d" || !sess.logout {
		t.Fatalf("session %+v", sess)
	}
	if sess.order[0] != "create" {
		t.Fatalf("must create before revoking: %v", sess.order)
	}
}

func TestPortalMinterFailedCreateRevokesNothing(t *testing.T) {
	name := auth.TokenName("1a2b3c4d")
	sess := &fakeSession{tokens: []portal.TokenInfo{{ID: "old", Name: name}}, createErr: loqed.ErrInvalidPayload}
	m := &auth.PortalMinter{
		Login:     func(context.Context, string, string) (auth.PortalSession, error) { return sess, nil },
		TokenName: name,
	}
	if _, err := m.Mint(context.Background()); !errors.Is(err, loqed.ErrInvalidPayload) {
		t.Fatalf("got %v", err)
	}
	if len(sess.revoked) != 0 || !sess.logout {
		t.Fatalf("session %+v", sess)
	}
}

func TestMintedTokenForAnotherAccountIsNotUsed(t *testing.T) {
	st := newStore(t)
	_ = st.Update(func(c *store.Cache) {
		c.Minted = &store.MintedToken{ID: "x", Value: "other-account", EmailSHA256: store.EmailHash("old@example.com")}
	})
	m := &fakeMinter{}
	tok, err := auth.NewResolver("", "me@example.com", m, st, time.Now, discard).Token(context.Background())
	if err != nil || tok != "minted-1" || m.calls != 1 {
		t.Fatalf("%q %v calls=%d", tok, err, m.calls)
	}
	if got := st.Snapshot().Minted; got.EmailSHA256 != store.EmailHash("ME@example.com") {
		t.Fatalf("minted token not tagged with the account: %+v", got)
	}
}

func TestEmailWithoutPasswordUsesCachedTokenButCannotMint(t *testing.T) {
	st := newStore(t)
	_ = st.Update(func(c *store.Cache) {
		c.Minted = &store.MintedToken{Value: "cached", EmailSHA256: store.EmailHash("me@example.com")}
	})
	r := auth.NewResolver("", "me@example.com", nil, st, time.Now, discard)
	if tok, err := r.Token(context.Background()); err != nil || tok != "cached" {
		t.Fatalf("%q %v", tok, err)
	}
	if _, err := r.Invalidate(context.Background(), "cached"); !errors.Is(err, auth.ErrNoToken) {
		t.Fatalf("got %v", err)
	}
}

func TestMintLimitSurvivesRestart(t *testing.T) {
	now := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	st := newStore(t)
	m := &fakeMinter{}
	first := auth.NewResolver("", "me@example.com", m, st, func() time.Time { return now }, discard)
	tok, _ := first.Token(context.Background())
	// A restarted process with the same cache must not mint again within the hour.
	second := auth.NewResolver("", "me@example.com", m, st, func() time.Time { return now.Add(time.Minute) }, discard)
	if _, err := second.Invalidate(context.Background(), tok); !errors.Is(err, loqed.ErrUnauthorized) || m.calls != 1 {
		t.Fatalf("err %v calls=%d", err, m.calls)
	}
}

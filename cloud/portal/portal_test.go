package portal_test

import (
	"context"
	"errors"
	"testing"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/cloud/portal"
)

func login(t *testing.T, setup ...func(*fakePortal)) (*fakePortal, *portal.Session) {
	t.Helper()
	f, srv := newFakePortal(t)
	for _, fn := range setup {
		f.set(fn)
	}
	s, err := portal.New(portal.WithBaseURL(srv.URL)).Login(context.Background(), "me@example.com", "s3cret")
	if err != nil {
		t.Fatal(err)
	}
	return f, s
}

func TestLoginRejectsBadPassword(t *testing.T) {
	_, srv := newFakePortal(t)
	_, err := portal.New(portal.WithBaseURL(srv.URL)).Login(context.Background(), "me@example.com", "wrong")
	if !errors.Is(err, loqed.ErrUnauthorized) {
		t.Fatalf("got %v", err)
	}
}

func TestLoginWithoutXSRFCookieUsesMetaToken(t *testing.T) {
	_, s := login(t, func(f *fakePortal) { f.noXSRFCookie = true })
	if _, err := s.CreateToken(context.Background(), "x"); err != nil {
		t.Fatal(err)
	}
}

func TestCSRFRejectionIsNotReportedAsBadPassword(t *testing.T) {
	f, srv := newFakePortal(t)
	f.set(func(f *fakePortal) { f.rejectCSRF = true })
	_, err := portal.New(portal.WithBaseURL(srv.URL)).Login(context.Background(), "me@example.com", "s3cret")
	if !errors.Is(err, loqed.ErrInvalidPayload) || errors.Is(err, loqed.ErrUnauthorized) {
		t.Fatalf("got %v", err)
	}
}

func TestCreateListRevokeToken(t *testing.T) {
	f, s := login(t)
	ctx := context.Background()

	tok, err := s.CreateToken(ctx, "loqed-mqtt 1a2b3c4d")
	if err != nil {
		t.Fatal(err)
	}
	if tok.Value != "pat-1" || tok.Name != "loqed-mqtt 1a2b3c4d" || tok.ID == "" {
		t.Fatalf("token %+v", tok)
	}

	list, err := s.ListTokens(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].ID != tok.ID || list[0].Name != tok.Name {
		t.Fatalf("list %+v", list)
	}

	if err := s.RevokeToken(ctx, tok.ID); err != nil {
		t.Fatal(err)
	}
	if names := f.tokenNames(); len(names) != 0 {
		t.Fatalf("tokens left: %v", names)
	}
	if err := s.Logout(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestCreateTokenFindsIDAmongSameNamedTokens(t *testing.T) {
	_, s := login(t)
	ctx := context.Background()
	first, err := s.CreateToken(ctx, "dup")
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.CreateToken(ctx, "dup")
	if err != nil {
		t.Fatal(err)
	}
	if first.ID == second.ID || second.Value != "pat-2" {
		t.Fatalf("first %+v second %+v", first, second)
	}
}

func TestVersionMismatchOnGetLoadsPage(t *testing.T) {
	f, s := login(t)
	f.set(func(f *fakePortal) { f.version = "v2" })
	if _, err := s.ListTokens(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// Inertia version-checks the GET that follows the create redirect. The
// create must not be re-sent and the flashed token must not be lost.
func TestVersionMismatchAfterCreateKeepsTokenAndDoesNotResend(t *testing.T) {
	f, s := login(t, func(f *fakePortal) { f.bumpOnCreate = true })
	tok, err := s.CreateToken(context.Background(), "once")
	if err != nil {
		t.Fatal(err)
	}
	if tok.Value != "pat-1" || tok.ID == "" {
		t.Fatalf("token %+v", tok)
	}
	f.mu.Lock()
	calls := f.createCalls
	f.mu.Unlock()
	if calls != 1 {
		t.Fatalf("create sent %d times", calls)
	}
}

func TestExpiredSessionIsUnauthorized(t *testing.T) {
	f, s := login(t)
	f.set(func(f *fakePortal) {
		for _, sess := range f.sessions {
			sess.authed = false
		}
	})
	if _, err := s.ListTokens(context.Background()); !errors.Is(err, loqed.ErrUnauthorized) {
		t.Fatalf("got %v", err)
	}
}

func TestUnexpectedPageIsInvalidPayload(t *testing.T) {
	f, s := login(t)
	f.set(func(f *fakePortal) { f.brokenTokensPage = true })
	if _, err := s.ListTokens(context.Background()); !errors.Is(err, loqed.ErrInvalidPayload) {
		t.Fatalf("got %v", err)
	}
}

func TestLoginUnreachable(t *testing.T) {
	_, err := portal.New(portal.WithBaseURL("http://127.0.0.1:1")).Login(context.Background(), "a", "b")
	if !errors.Is(err, loqed.ErrUnreachable) {
		t.Fatalf("got %v", err)
	}
}

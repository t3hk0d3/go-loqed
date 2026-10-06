// Package auth decides which LOQED cloud token to use and mints new ones
// through the Integrations portal when email/password are configured.
package auth

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/cloud/portal"
	"github.com/t3hk0d3/go-loqed/internal/store"
)

var ErrNoToken = errors.New("auth: no LOQED token available; set cloud_token, or cloud_email and cloud_password")

// RemintInterval limits how often a token is minted (persisted, so it also
// holds across restarts).
const RemintInterval = time.Hour

type Minter interface {
	Mint(ctx context.Context) (store.MintedToken, error)
}

// PortalSession is the part of *portal.Session the minter needs.
type PortalSession interface {
	ListTokens(ctx context.Context) ([]portal.TokenInfo, error)
	CreateToken(ctx context.Context, name string) (portal.Token, error)
	RevokeToken(ctx context.Context, id string) error
	Logout(ctx context.Context) error
}

type PortalMinter struct {
	Login     func(ctx context.Context, email, password string) (PortalSession, error)
	Email     string
	Password  string
	TokenName string
	Log       *slog.Logger
}

func NewPortalMinter(c *portal.Client, email, password, tokenName string, log *slog.Logger) *PortalMinter {
	return &PortalMinter{
		Login: func(ctx context.Context, email, password string) (PortalSession, error) {
			s, err := c.Login(ctx, email, password)
			if err != nil {
				return nil, err
			}
			return s, nil
		},
		Email: email, Password: password, TokenName: tokenName, Log: log,
	}
}

// TokenName names minted tokens after the installation id, which is
// stable across container re-creation (a Docker hostname is not).
func TokenName(installID string) string { return "loqed-mqtt " + installID }

// Mint logs in, creates a new token, then revokes older tokens with the
// same name and logs out. Creating first means a failed create never
// leaves the user without a working token.
func (m *PortalMinter) Mint(ctx context.Context) (store.MintedToken, error) {
	s, err := m.Login(ctx, m.Email, m.Password)
	if err != nil {
		return store.MintedToken{}, err
	}
	defer func() { _ = s.Logout(context.WithoutCancel(ctx)) }()
	tok, err := s.CreateToken(ctx, m.TokenName)
	if err != nil {
		return store.MintedToken{}, err
	}
	existing, err := s.ListTokens(ctx)
	if err != nil {
		m.logWarn("could not list old LOQED tokens to revoke", err)
		return store.MintedToken{ID: tok.ID, Value: tok.Value}, nil
	}
	for _, t := range existing {
		if t.Name == m.TokenName && t.ID != tok.ID && tok.ID != "" {
			if err := s.RevokeToken(ctx, t.ID); err != nil {
				m.logWarn("could not revoke an old LOQED token", err)
			}
		}
	}
	return store.MintedToken{ID: tok.ID, Value: tok.Value}, nil
}

func (m *PortalMinter) logWarn(msg string, err error) {
	if m.Log != nil {
		m.Log.Warn(msg, "err", err)
	}
}

type Resolver struct {
	configured string
	emailHash  string // "" when cloud_email is not configured
	minter     Minter // nil when minting is impossible
	store      *store.Store
	now        func() time.Time
	log        *slog.Logger

	mu sync.Mutex

	warnMu         sync.Mutex
	lastExpiryWarn time.Time
}

// NewResolver: configured is cloud_token; email is cloud_email; minter is
// nil unless both cloud_email and cloud_password are set.
func NewResolver(configured, email string, minter Minter, st *store.Store, now func() time.Time, log *slog.Logger) *Resolver {
	r := &Resolver{configured: configured, minter: minter, store: st, now: now, log: log}
	if email != "" {
		r.emailHash = store.EmailHash(email)
	}
	return r
}

// Token returns cloud_token, else the cached minted token (if it belongs
// to the configured account), else mints one.
func (r *Resolver) Token(ctx context.Context) (string, error) {
	if r.configured != "" {
		return r.configured, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if v := r.cachedLocked(); v != "" {
		return v, nil
	}
	return r.mintLocked(ctx)
}

// Invalidate reports that rejected was refused by the cloud and returns a
// replacement, minting at most once per RemintInterval.
func (r *Resolver) Invalidate(ctx context.Context, rejected string) (string, error) {
	if r.configured != "" {
		return "", fmt.Errorf("%w: the configured cloud_token was rejected; create a new one at https://integrations.loqed.com/personal-access-tokens", loqed.ErrUnauthorized)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if v := r.cachedLocked(); v != "" && v != rejected {
		return v, nil
	}
	return r.mintLocked(ctx)
}

func (r *Resolver) cachedLocked() string {
	m := r.store.Snapshot().Minted
	if m == nil || m.Value == "" {
		return ""
	}
	if r.emailHash != "" && m.EmailSHA256 != r.emailHash {
		return "" // minted for a different account
	}
	return m.Value
}

func (r *Resolver) mintLocked(ctx context.Context) (string, error) {
	if r.minter == nil {
		if r.emailHash != "" {
			return "", fmt.Errorf("%w (cloud_password is needed to create a token for cloud_email)", ErrNoToken)
		}
		return "", ErrNoToken
	}
	now := r.now()
	if last := r.store.Snapshot().LastMintAt; !last.IsZero() && now.Sub(last) < RemintInterval && !now.Before(last) {
		return "", fmt.Errorf("%w: token rejected; next attempt to create one after %s",
			loqed.ErrUnauthorized, last.Add(RemintInterval).Format(time.RFC3339))
	}
	// Persist the attempt before minting so restarts cannot bypass the limit.
	if err := r.store.Update(func(c *store.Cache) { c.LastMintAt = now }); err != nil {
		r.log.Warn("could not save the token mint time", "err", err)
	}
	tok, err := r.minter.Mint(ctx)
	if err != nil {
		return "", fmt.Errorf("auth: creating a token with cloud_email/cloud_password failed (set cloud_token to bypass): %w", err)
	}
	tok.EmailSHA256, tok.MintedAt = r.emailHash, now
	if err := r.store.Update(func(c *store.Cache) { c.Minted = &tok }); err != nil {
		r.log.Warn("could not save the new LOQED token", "err", err)
	}
	r.log.Info("created a LOQED personal access token", "token_id", tok.ID)
	return tok.Value, nil
}

package gateway

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/cloud"
)

type CloudAPI interface {
	ListLocks(ctx context.Context) ([]cloud.Lock, error)
	Command(ctx context.Context, lockID string, s loqed.BoltState) error
}

type TokenSource interface {
	Token(ctx context.Context) (string, error)
	Invalidate(ctx context.Context, rejected string) (string, error)
	// KeyDeleted reports that token's lock key was deleted in the LOQED app
	// and returns a replacement (with a new key) for the next request.
	KeyDeleted(ctx context.Context, token string) (string, error)
}

const (
	RateLimitBackoff = 12 * time.Hour
	coalesceWindow   = 30 * time.Second
	cloudTimeout     = 15 * time.Second
)

// LockList is one GET /api/locks/ result. FetchedAt is when the request
// was sent: the data is at least that fresh.
type LockList struct {
	Locks     []cloud.Lock
	FetchedAt time.Time
}

// CloudHub serializes cloud reads: one ListLocks result serves every lock,
// reads are budgeted, and rejected tokens are replaced once. Commands do
// not wait for reads; no lock is held during network calls.
type CloudHub struct {
	budget *Budget
	tokens TokenSource
	newAPI func(token string) CloudAPI
	now    func() time.Time
	log    *slog.Logger

	reads chan struct{} // one ListLocks at a time (ctx-aware)

	mu    sync.Mutex
	api   CloudAPI
	token string
	last  LockList
}

func NewCloudHub(b *Budget, tokens TokenSource, newAPI func(token string) CloudAPI, now func() time.Time, log *slog.Logger) *CloudHub {
	return &CloudHub{budget: b, tokens: tokens, newAPI: newAPI, now: now, log: log, reads: make(chan struct{}, 1)}
}

// Token is the token currently in use ("" before the first call).
func (h *CloudHub) Token() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.token
}

// ResetToken makes the next request ask the token source again (after the
// token was replaced elsewhere, for example before it expires).
func (h *CloudHub) ResetToken() {
	h.mu.Lock()
	h.api, h.token = nil, ""
	h.mu.Unlock()
}

// Budget exposes the shared budget (for spacing and diagnostics).
func (h *CloudHub) Budget() *Budget { return h.budget }

// Locks returns the account's locks. A result fetched within the last 30 s
// is shared, but only if it was fetched at or after notBefore (pass the
// command time for confirmation polls, zero otherwise). Callers must not
// modify the slice.
func (h *CloudHub) Locks(ctx context.Context, p Priority, notBefore time.Time) (LockList, error) {
	select {
	case h.reads <- struct{}{}:
	case <-ctx.Done():
		return LockList{}, ctx.Err()
	}
	defer func() { <-h.reads }()

	h.mu.Lock()
	last := h.last
	h.mu.Unlock()
	now := h.now()
	if last.Locks != nil && now.Sub(last.FetchedAt) < coalesceWindow && !last.FetchedAt.Before(notBefore) {
		return last, nil
	}
	api, tok, err := h.client(ctx)
	if err != nil {
		return LockList{}, err
	}
	if err := h.budget.Take(p); err != nil {
		return LockList{}, err
	}
	started := h.now()
	locks, err := listLocks(ctx, api)
	if errors.Is(err, loqed.ErrUnauthorized) {
		// LOQED does not count reads it rejects (spec 2.5, V12), so neither
		// do we: a wrong token must not lock the gateway out for 12 h.
		h.budget.Refund()
		api, rerr := h.reauth(ctx, tok)
		if rerr != nil {
			return LockList{}, errors.Join(err, rerr)
		}
		if err = h.budget.Take(p); err == nil {
			started = h.now()
			locks, err = listLocks(ctx, api)
			if errors.Is(err, loqed.ErrUnauthorized) {
				h.budget.Refund()
			}
		}
	}
	if errors.Is(err, loqed.ErrRateLimited) {
		h.budget.Block(RateLimitBackoff)
		h.log.Error("LOQED cloud rate limit reached; cloud reads suspended for 12h", "err", err)
	}
	if err != nil {
		return LockList{}, err
	}
	res := LockList{Locks: locks, FetchedAt: started}
	h.mu.Lock()
	h.last = res
	h.mu.Unlock()
	return res, nil
}

// Command sends a door command. Commands are outside the budget: LOQED's
// 12-per-12h limit only applies to status reads (V2). A 401 is
// retried once with a replacement token: a rejected request did nothing.
// A deleted lock key is never retried.
func (h *CloudHub) Command(ctx context.Context, lockID string, s loqed.BoltState) error {
	api, tok, err := h.client(ctx)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrNoCloudAccess, err)
	}
	err = h.command(ctx, api, lockID, s)
	if errors.Is(err, loqed.ErrUnauthorized) {
		api, rerr := h.reauth(ctx, tok)
		if rerr != nil {
			return errors.Join(err, rerr)
		}
		err = h.command(ctx, api, lockID, s)
	}
	if errors.Is(err, cloud.ErrKeyDeleted) {
		// The cloud answered, so the command is never resent; the next
		// command uses a token with a working key, if one can be had.
		h.log.Error("the LOQED cloud refused the command: this token's lock key was deleted in the LOQED app")
		if tok2, rerr := h.tokens.KeyDeleted(ctx, h.Token()); rerr != nil {
			h.log.Warn("no replacement token for cloud commands", "err", rerr)
		} else {
			h.mu.Lock()
			if h.token != tok2 {
				h.token, h.api = tok2, h.newAPI(tok2)
			}
			h.mu.Unlock()
		}
	}
	return err
}

func (h *CloudHub) command(ctx context.Context, api CloudAPI, lockID string, s loqed.BoltState) error {
	h.log.Info("sending cloud command", "lock_id", lockID, "state", s)
	ctx, cancel := context.WithTimeout(ctx, cloudTimeout)
	defer cancel()
	return api.Command(ctx, lockID, s)
}

func (h *CloudHub) client(ctx context.Context) (CloudAPI, string, error) {
	h.mu.Lock()
	api, tok := h.api, h.token
	h.mu.Unlock()
	if api != nil {
		return api, tok, nil
	}
	tok, err := h.tokens.Token(ctx) // the resolver serializes minting
	if err != nil {
		return nil, "", err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.api == nil || h.token != tok {
		h.token, h.api = tok, h.newAPI(tok)
	}
	return h.api, h.token, nil
}

func (h *CloudHub) reauth(ctx context.Context, rejected string) (CloudAPI, error) {
	tok, err := h.tokens.Invalidate(ctx, rejected)
	if err != nil {
		return nil, err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.token != tok {
		h.token, h.api = tok, h.newAPI(tok)
	}
	return h.api, nil
}

func listLocks(ctx context.Context, api CloudAPI) ([]cloud.Lock, error) {
	ctx, cancel := context.WithTimeout(ctx, cloudTimeout)
	defer cancel()
	return api.ListLocks(ctx)
}

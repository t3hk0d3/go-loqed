package auth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
)

// ExpiryWarning is how long before a token expires the gateway starts
// warning (and replaces a minted token).
const ExpiryWarning = 14 * 24 * time.Hour

const expiryWarnInterval = 24 * time.Hour

// TokenExpiry reads the exp claim of a LOQED personal access token (an
// RS256 JWT). The signature is not checked: the value is only used to warn
// and to re-mint early. It reports false for anything it cannot read.
func TokenExpiry(token string) (time.Time, bool) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return time.Time{}, false
	}
	payload, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(parts[1], "="))
	if err != nil {
		return time.Time{}, false
	}
	var claims struct {
		Exp *json.Number `json:"exp"`
	}
	if json.Unmarshal(payload, &claims) != nil || claims.Exp == nil {
		return time.Time{}, false
	}
	exp, err := claims.Exp.Int64()
	if err != nil {
		f, ferr := claims.Exp.Float64()
		if ferr != nil {
			return time.Time{}, false
		}
		exp = int64(f)
	}
	return time.Unix(exp, 0).UTC(), true
}

// Expiry is the expiry of the token currently in use, without network I/O.
func (r *Resolver) Expiry() (time.Time, bool) {
	tok := r.configured
	if tok == "" {
		r.mu.Lock()
		tok = r.cachedLocked()
		r.mu.Unlock()
	}
	if tok == "" {
		return time.Time{}, false
	}
	return TokenExpiry(tok)
}

// CheckExpiry warns (at most daily) about a token expiring within
// ExpiryWarning and replaces a minted one. Minting creates a new lock key,
// so it only happens when needed, never on a schedule.
func (r *Resolver) CheckExpiry(ctx context.Context) (reminted bool, err error) {
	exp, ok := r.Expiry()
	now := r.now()
	if !ok || exp.Sub(now) > ExpiryWarning {
		return false, nil
	}
	if r.configured != "" {
		r.warnExpiry(now, "the configured cloud_token expires soon; create a new one at https://integrations.loqed.com/personal-access-tokens", exp)
		return false, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.minter == nil {
		r.warnExpiry(now, "the LOQED token expires soon and cannot be replaced without cloud_password", exp)
		return false, nil
	}
	if _, err := r.mintLocked(ctx); err != nil {
		r.warnExpiry(now, "the LOQED token expires soon and replacing it failed", exp, "err", err)
		return false, err
	}
	r.log.Info("replaced the LOQED token before it expires", "old_expires_at", exp.Format(time.RFC3339))
	return true, nil
}

func (r *Resolver) warnExpiry(now time.Time, msg string, exp time.Time, args ...any) {
	r.warnMu.Lock()
	defer r.warnMu.Unlock()
	if !r.lastExpiryWarn.IsZero() && now.Sub(r.lastExpiryWarn) < expiryWarnInterval {
		return
	}
	r.lastExpiryWarn = now
	r.log.Warn(msg, append([]any{"expires_at", exp.Format(time.RFC3339)}, args...)...)
}

// KeyDeleted reports that token's lock key was deleted in the LOQED app (the
// cloud refuses commands with it) and returns a replacement token, which
// comes with a new key. A configured cloud_token cannot be replaced.
func (r *Resolver) KeyDeleted(ctx context.Context, token string) (string, error) {
	if r.configured != "" {
		return "", fmt.Errorf("%w: the lock key of the configured cloud_token was deleted in the LOQED app; create a new token", loqed.ErrUnauthorized)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if v := r.cachedLocked(); v != "" && v != token {
		return v, nil
	}
	return r.mintLocked(ctx)
}

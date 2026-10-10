package loqed

import (
	"errors"
	"fmt"
)

// Sentinel errors returned (wrapped) by every GoLoqed client. Test for them
// with errors.Is; the message text is not part of the API.
var (
	// ErrUnauthorized means the credentials (token, bridge key, key secret,
	// password) were rejected, or the server redirected to its login page.
	ErrUnauthorized = errors.New("loqed: unauthorized")
	// ErrRateLimited means LOQED refused the request because of rate
	// limiting (HTTP 429).
	ErrRateLimited = errors.New("loqed: rate limited")
	// ErrUnreachable means the request was provably not delivered (DNS, dial
	// or connect failure, including a connect timeout). It is the only
	// network error after which a command may be sent again.
	ErrUnreachable = errors.New("loqed: unreachable")
	// ErrNoResponse means the request may have been delivered but no
	// complete response arrived (timeout after sending, connection reset,
	// truncated body). The remote side may have acted on it: never resend a
	// command after this error.
	ErrNoResponse = errors.New("loqed: no response")
	// ErrBadSignature means an incoming bridge webhook had a missing,
	// malformed or wrong HASH/TIMESTAMP.
	ErrBadSignature = errors.New("loqed: bad signature")
	// ErrStaleTimestamp means an incoming bridge webhook was authentic but
	// its TIMESTAMP is outside the allowed window (late delivery, clock skew
	// or a replay).
	ErrStaleTimestamp = errors.New("loqed: stale timestamp")
	// ErrInvalidPayload means a response or webhook body could not be
	// understood.
	ErrInvalidPayload = errors.New("loqed: invalid payload")
)

// APIError is any other non-success HTTP response.
type APIError struct {
	StatusCode int
	Body       string // truncated; never contains request secrets
}

// Error describes the status code and the truncated body.
func (e *APIError) Error() string {
	return fmt.Sprintf("loqed: unexpected HTTP status %d: %s", e.StatusCode, e.Body)
}

// IsServerError reports whether err wraps an APIError with a 5xx status.
func IsServerError(err error) bool {
	var apiErr *APIError
	return errors.As(err, &apiErr) && apiErr.StatusCode >= 500
}

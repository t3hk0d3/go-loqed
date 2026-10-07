package loqed

import (
	"errors"
	"fmt"
)

var (
	// ErrUnauthorized: credentials (token, bridge key, key secret, password) were rejected.
	ErrUnauthorized = errors.New("loqed: unauthorized")
	// ErrRateLimited: LOQED refused the request because of rate limiting.
	ErrRateLimited = errors.New("loqed: rate limited")
	// ErrUnreachable: the request was provably not delivered (DNS, dial or
	// connect failure, including a connect timeout). Safe to retry elsewhere.
	ErrUnreachable = errors.New("loqed: unreachable")
	// ErrNoResponse: the request may have been delivered but no complete
	// response arrived (timeout after sending, connection reset, truncated
	// body). The remote side may have acted on it; never blindly resend.
	ErrNoResponse = errors.New("loqed: no response")
	// ErrBadSignature: an incoming webhook had a missing or wrong HASH/TIMESTAMP.
	ErrBadSignature = errors.New("loqed: bad signature")
	// ErrStaleTimestamp: an incoming webhook timestamp is outside the allowed window.
	ErrStaleTimestamp = errors.New("loqed: stale timestamp")
	// ErrInvalidPayload: a response or webhook body could not be understood.
	ErrInvalidPayload = errors.New("loqed: invalid payload")
)

// APIError is any other non-success HTTP response.
type APIError struct {
	StatusCode int
	Body       string // truncated; never contains request secrets
}

func (e *APIError) Error() string {
	return fmt.Sprintf("loqed: unexpected HTTP status %d: %s", e.StatusCode, e.Body)
}

// IsServerError reports whether err wraps an APIError with a 5xx status.
func IsServerError(err error) bool {
	var apiErr *APIError
	return errors.As(err, &apiErr) && apiErr.StatusCode >= 500
}

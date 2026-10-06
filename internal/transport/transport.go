// Package transport maps HTTP results onto GoLoqed's sentinel errors.
package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"sync/atomic"

	loqed "github.com/t3hk0d3/go-loqed"
)

// MaxBody caps how much of any response is read.
const MaxBody = 1 << 20

const maxErrorBody = 256

// Do sends req and returns the body of a 2xx response. Failures map to
// loqed sentinels:
//
//   - ErrUnreachable: the request was never written to a connection, so the
//     remote side cannot have acted on it;
//   - ErrNoResponse: the request was written but no complete response came
//     back (it may have been executed).
//
// The request URL is never included in errors because it may carry a signed
// command.
func Do(hc *http.Client, req *http.Request) ([]byte, error) {
	resp, body, err := Send(hc, req)
	if err != nil {
		return nil, err
	}
	if err := CheckStatus(resp.StatusCode, body); err != nil {
		return nil, err
	}
	return body, nil
}

// Send is Do without status mapping: it returns the response (body already
// read, at most MaxBody, and closed) for callers that need headers or
// non-2xx statuses. Transport errors are classified like Do.
func Send(hc *http.Client, req *http.Request) (*http.Response, []byte, error) {
	var wrote atomic.Bool
	trace := &httptrace.ClientTrace{
		WroteRequest: func(info httptrace.WroteRequestInfo) {
			if info.Err == nil {
				wrote.Store(true)
			}
		},
	}
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))
	resp, err := hc.Do(req)
	if err != nil {
		return nil, nil, requestError(err, wrote.Load())
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, MaxBody))
	if err != nil {
		return nil, nil, fmt.Errorf("%w: reading response: %w", loqed.ErrNoResponse, stripURL(err))
	}
	return resp, body, nil
}

// requestError classifies an error returned by http.Client.Do. wrote reports
// whether the request had been fully written. The URL is stripped.
func requestError(err error, wrote bool) error {
	err = stripURL(err)
	switch {
	case wrote:
		return fmt.Errorf("%w: %w", loqed.ErrNoResponse, err)
	case errors.Is(err, context.Canceled):
		return fmt.Errorf("loqed: request canceled: %w", err)
	default:
		return fmt.Errorf("%w: %w", loqed.ErrUnreachable, err)
	}
}

// stripURL unwraps *url.Error, whose message embeds the full request URL.
func stripURL(err error) error {
	var ue *url.Error
	if errors.As(err, &ue) {
		return ue.Err
	}
	return err
}

// CheckStatus converts a non-2xx status into an error. Redirects count as
// unauthorized: LOQED's Laravel backend redirects unauthenticated API calls
// to /login.
func CheckStatus(code int, body []byte) error {
	switch {
	case code >= 200 && code < 300:
		return nil
	case code == http.StatusUnauthorized, code == http.StatusForbidden:
		return fmt.Errorf("%w: HTTP %d", loqed.ErrUnauthorized, code)
	case code >= 300 && code < 400:
		return fmt.Errorf("%w: redirected to login (HTTP %d)", loqed.ErrUnauthorized, code)
	case code == http.StatusTooManyRequests:
		return fmt.Errorf("%w: HTTP %d", loqed.ErrRateLimited, code)
	default:
		if len(body) > maxErrorBody {
			body = body[:maxErrorBody]
		}
		return &loqed.APIError{StatusCode: code, Body: string(body)}
	}
}

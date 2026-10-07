package transport_test

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/internal/transport"
)

const secretPath = "/to_lock?command_signed_base64=SECRET"

func serve(t *testing.T, code int, body string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if code >= 300 && code < 400 {
			w.Header().Set("Location", "/login")
		}
		w.WriteHeader(code)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func get(t *testing.T, url string) ([]byte, error) {
	t.Helper()
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	hc := &http.Client{
		Timeout:       time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	return transport.Do(hc, req)
}

func noLeak(t *testing.T, err error) {
	t.Helper()
	if err != nil && strings.Contains(err.Error(), "SECRET") {
		t.Fatalf("error leaks URL: %v", err)
	}
}

func TestDoSuccess(t *testing.T) {
	body, err := get(t, serve(t, 200, "ok").URL)
	if err != nil || string(body) != "ok" {
		t.Fatalf("got %q, %v", body, err)
	}
}

func TestDoMapsStatusCodes(t *testing.T) {
	cases := []struct {
		code int
		want error
	}{
		{401, loqed.ErrUnauthorized},
		{403, loqed.ErrUnauthorized},
		{302, loqed.ErrUnauthorized},
		{429, loqed.ErrRateLimited},
	}
	for _, c := range cases {
		_, err := get(t, serve(t, c.code, "").URL)
		if !errors.Is(err, c.want) {
			t.Errorf("%d: got %v want %v", c.code, err, c.want)
		}
	}
}

func TestDoOtherStatusIsAPIErrorWithTruncatedBody(t *testing.T) {
	_, err := get(t, serve(t, 502, strings.Repeat("x", 1000)).URL)
	var apiErr *loqed.APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != 502 || len(apiErr.Body) > 256 {
		t.Fatalf("got %v", err)
	}
	if !loqed.IsServerError(err) {
		t.Fatal("expected server error")
	}
}

func TestDoConnectionRefusedIsUnreachable(t *testing.T) {
	srv := serve(t, 200, "")
	srv.Close()
	_, err := get(t, srv.URL+secretPath)
	if !errors.Is(err, loqed.ErrUnreachable) || errors.Is(err, loqed.ErrNoResponse) {
		t.Fatalf("got %v", err)
	}
	noLeak(t, err)
}

// A server that reads the request and never answers: the request was
// delivered, so the outcome is unknown.
func TestDoTimeoutAfterSendIsNoResponse(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				buf := make([]byte, 4096)
				_, _ = c.Read(buf) // read the request, then hang
				time.Sleep(3 * time.Second)
				_ = c.Close()
			}()
		}
	}()
	_, err = get(t, "http://"+ln.Addr().String()+secretPath)
	if !errors.Is(err, loqed.ErrNoResponse) || errors.Is(err, loqed.ErrUnreachable) {
		t.Fatalf("got %v", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) && !strings.Contains(err.Error(), "Timeout") {
		t.Fatalf("timeout cause lost: %v", err)
	}
	noLeak(t, err)
}

// A server that closes the connection after reading the request.
func TestDoResetAfterSendIsNoResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			_ = conn.Close()
		}
	}))
	t.Cleanup(srv.Close)
	_, err := get(t, srv.URL+secretPath)
	if !errors.Is(err, loqed.ErrNoResponse) {
		t.Fatalf("got %v", err)
	}
	noLeak(t, err)
}

func TestDoCanceledContextIsNotUnreachable(t *testing.T) {
	srv := serve(t, 200, "")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+secretPath, nil)
	_, err := transport.Do(http.DefaultClient, req)
	if !errors.Is(err, context.Canceled) || errors.Is(err, loqed.ErrUnreachable) || errors.Is(err, loqed.ErrNoResponse) {
		t.Fatalf("got %v", err)
	}
	noLeak(t, err)
}

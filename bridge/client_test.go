package bridge_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/bridge"
)

// A host that takes over the bridge IP must not be able to bounce the
// gateway's requests (signed commands, signed webhook changes) elsewhere, and
// a redirect is an answer: it must never be treated as "not delivered" or
// "unauthorized", both of which allow a command to be sent again.
func TestRedirectIsNeverFollowedAndIsAnError(t *testing.T) {
	calls := []struct {
		name string
		call func(*bridge.Client) error
	}{
		{"command", func(c *bridge.Client) error { return c.Command(context.Background(), bridge.ActionOpen) }},
		{"status", func(c *bridge.Client) error { _, err := c.Status(context.Background()); return err }},
		{"list webhooks", func(c *bridge.Client) error { _, err := c.ListWebhooks(context.Background()); return err }},
		{"create webhook", func(c *bridge.Client) error {
			return c.CreateWebhook(context.Background(), "http://10.0.0.5:8099/webhook/lock1", bridge.AllTriggers)
		}},
		{"delete webhook", func(c *bridge.Client) error { return c.DeleteWebhook(context.Background(), 7) }},
	}
	for _, code := range []int{http.StatusFound, http.StatusTemporaryRedirect, http.StatusPermanentRedirect} {
		for _, tc := range calls {
			t.Run(tc.name+"/"+http.StatusText(code), func(t *testing.T) {
				var followed atomic.Int32
				target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
					followed.Add(1)
				}))
				t.Cleanup(target.Close)
				c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					http.Redirect(w, r, target.URL+"/LOCATIONSECRET", code)
				}))

				err := tc.call(c)

				if followed.Load() != 0 {
					t.Fatal("redirect target was requested")
				}
				if err == nil {
					t.Fatal("expected an error")
				}
				var apiErr *loqed.APIError
				if !errors.As(err, &apiErr) || apiErr.StatusCode != code {
					t.Fatalf("want APIError %d, got %v", code, err)
				}
				for _, retryable := range []error{loqed.ErrUnreachable, loqed.ErrUnauthorized, loqed.ErrNoResponse} {
					if errors.Is(err, retryable) {
						t.Fatalf("redirect classified as %v: %v", retryable, err)
					}
				}
				if strings.Contains(err.Error(), "LOCATIONSECRET") {
					t.Fatalf("error leaks redirect target: %v", err)
				}
			})
		}
	}
}

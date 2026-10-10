package bridge

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/internal/transport"
)

// DefaultTimeout bounds every bridge request.
const DefaultTimeout = 5 * time.Second

// Credentials are the per-lock secrets from the cloud lock list.
type Credentials struct {
	BridgeKey  string // base64; webhook management and incoming webhook signatures
	KeySecret  string // base64; signs lock commands
	LocalKeyID uint8
}

// Client talks to one bridge.
type Client struct {
	base      string
	hc        *http.Client
	now       func() time.Time
	bridgeKey []byte
	keySecret []byte
	localID   uint8
}

// Option configures a Client.
type Option func(*Client)

// WithHTTPClient replaces the default client. The injected client must set
// DisableKeepAlives on its transport, otherwise net/http may silently replay
// a GET (a signed lock command) on a reused connection, and must not follow
// redirects (CheckRedirect returning http.ErrUseLastResponse): following one
// re-sends the signed request to wherever the answering host points it.
func WithHTTPClient(hc *http.Client) Option { return func(c *Client) { c.hc = hc } }

// WithClock overrides the time source used to sign requests and check webhooks.
func WithClock(now func() time.Time) Option { return func(c *Client) { c.now = now } }

// WithBaseURL overrides the bridge base URL (for tests).
func WithBaseURL(base string) Option {
	return func(c *Client) { c.base = strings.TrimRight(base, "/") }
}

// New creates a client for the bridge at host (an IP address, optionally
// with :port; hostnames are rejected). Empty credentials are allowed; only
// Status works without them.
func New(host string, creds Credentials, opts ...Option) (*Client, error) {
	if !validHost(host) {
		return nil, fmt.Errorf("bridge: %q is not an IP address or IP:port", host)
	}
	bk, err := base64.StdEncoding.DecodeString(creds.BridgeKey)
	if err != nil {
		return nil, fmt.Errorf("bridge: invalid bridge key: %w", err)
	}
	ks, err := base64.StdEncoding.DecodeString(creds.KeySecret)
	if err != nil {
		return nil, fmt.Errorf("bridge: invalid key secret: %w", err)
	}
	c := &Client{
		base: "http://" + host,
		hc: &http.Client{
			Timeout: DefaultTimeout,
			// Fresh connection per request, so net/http never silently
			// replays a GET (a signed lock command) on a reused connection.
			Transport: &http.Transport{DisableKeepAlives: true},
			// The bridge never redirects; following one would re-send the
			// signed request to another host.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		now:       time.Now,
		bridgeKey: bk,
		keySecret: ks,
		localID:   creds.LocalKeyID,
	}
	for _, o := range opts {
		o(c)
	}
	return c, nil
}

// BridgeKey returns a copy of the decoded bridge key (for webhook verification).
func (c *Client) BridgeKey() []byte { return bytes.Clone(c.bridgeKey) }

// Status fetches GET /status. LOQED asks integrators to call it rarely.
func (c *Client) Status(ctx context.Context) (*Status, error) {
	body, err := c.do(ctx, http.MethodGet, "/status", nil, nil)
	if err != nil {
		return nil, err
	}
	var st Status
	if err := json.Unmarshal(body, &st); err != nil {
		return nil, fmt.Errorf("%w: status: %w", loqed.ErrInvalidPayload, err)
	}
	if st.BoltState == "" {
		st.BoltState = loqed.BoltUnknown
	}
	return &st, nil
}

func validHost(host string) bool {
	if _, err := netip.ParseAddr(host); err == nil {
		return true
	}
	h, _, err := net.SplitHostPort(host)
	if err != nil {
		return false
	}
	_, err = netip.ParseAddr(h)
	return err == nil
}

// do sends a request. header keys are set verbatim (not canonicalized):
// the bridge expects upper-case TIMESTAMP and HASH.
func (c *Client) do(ctx context.Context, method, path string, body []byte, header map[string]string) ([]byte, error) {
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, rd)
	if err != nil {
		// The parse error would quote the URL, which may hold a signed command.
		return nil, errors.New("bridge: invalid bridge address")
	}
	for k, v := range header {
		req.Header[k] = []string{v}
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, respBody, err := transport.Send(c.hc, req)
	if err != nil {
		return nil, err
	}
	// The bridge never redirects. A redirect is still an answer, so it must
	// not look unreachable or unauthorized (both let a command be sent
	// again); its body and Location may point anywhere and are dropped.
	if resp.StatusCode >= 300 && resp.StatusCode < 400 {
		return nil, &loqed.APIError{StatusCode: resp.StatusCode, Body: "unexpected redirect"}
	}
	if err := transport.CheckStatus(resp.StatusCode, respBody); err != nil {
		return nil, err
	}
	return respBody, nil
}

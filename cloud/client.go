// Package cloud is a stateless client for the LOQED cloud Lock API
// (https://integrations.production.loqed.com/api).
package cloud

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/internal/transport"
)

// DefaultBaseURL is the production Integrations host.
const DefaultBaseURL = "https://integrations.production.loqed.com"

// Client calls the Lock API with a personal access token.
type Client struct {
	base  string
	token string
	hc    *http.Client
}

// Option configures a Client.
type Option func(*Client)

// WithBaseURL overrides DefaultBaseURL (for tests or proxies).
func WithBaseURL(u string) Option { return func(c *Client) { c.base = strings.TrimRight(u, "/") } }

// WithHTTPClient replaces the default client. The injected client must set
// DisableKeepAlives on its transport (otherwise net/http may silently replay a
// GET command on a reused connection) and must keep redirects disabled: the
// API redirects unauthenticated calls to an HTML login page.
func WithHTTPClient(hc *http.Client) Option { return func(c *Client) { c.hc = hc } }

// New creates a client that authenticates with the personal access token.
func New(token string, opts ...Option) *Client {
	c := &Client{
		base:  DefaultBaseURL,
		token: token,
		hc: &http.Client{
			Timeout: 15 * time.Second,
			// Fresh connection per request, so net/http never silently replays
			// a GET (a lock command) on a reused connection.
			Transport:     &http.Transport{Proxy: http.ProxyFromEnvironment, DisableKeepAlives: true},
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// Lock is one entry of GET /api/locks/. Local fields (bridge IP and keys)
// are undocumented and may be empty.
type Lock struct {
	ID                  string
	Name                string
	ModelName           string
	BatteryPercentage   int
	BatteryType         string
	BoltState           loqed.BoltState
	PartyMode           bool
	GuestAccessMode     bool
	TwistAssist         bool
	TouchToConnect      bool
	LockDirection       string
	MortiseLockType     string
	SupportedLockStates []string
	Online              *bool
	BridgeIP            string
	BridgeHostname      string
	BridgeMacWifi       string
	LocalID             *int
	KeySecret           string
	BridgeKey           string
	BackendKey          string
}

// HasLocalCredentials reports whether the lock can be driven via its bridge.
func (l Lock) HasLocalCredentials() bool {
	return l.BridgeIP != "" && l.BridgeKey != "" && l.KeySecret != "" &&
		l.LocalID != nil && *l.LocalID >= 0 && *l.LocalID <= 255
}

type rawLock struct {
	ID                  loqed.String    `json:"id"`
	Name                string          `json:"name"`
	ModelName           string          `json:"model_name"`
	BatteryPercentage   loqed.Int       `json:"battery_percentage"`
	BatteryType         string          `json:"battery_type"`
	BoltState           loqed.BoltState `json:"bolt_state"`
	PartyMode           loqed.Bool      `json:"party_mode"`
	GuestAccessMode     loqed.Bool      `json:"guest_access_mode"`
	TwistAssist         loqed.Bool      `json:"twist_assist"`
	TouchToConnect      loqed.Bool      `json:"touch_to_connect"`
	LockDirection       string          `json:"lock_direction"`
	MortiseLockType     string          `json:"mortise_lock_type"`
	SupportedLockStates []string        `json:"supported_lock_states"`
	Online              *loqed.Bool     `json:"online"`
	BridgeIP            string          `json:"bridge_ip"`
	BridgeHostname      string          `json:"bridge_hostname"`
	BridgeMacWifi       string          `json:"bridge_mac_wifi"`
	LocalID             *loqed.Int      `json:"local_id"`
	KeySecret           string          `json:"key_secret"`
	BridgeKey           string          `json:"bridge_key"`
	BackendKey          string          `json:"backend_key"`
}

func (r rawLock) lock() Lock {
	l := Lock{
		ID: string(r.ID), Name: r.Name, ModelName: r.ModelName,
		BatteryPercentage: int(r.BatteryPercentage), BatteryType: r.BatteryType, BoltState: r.BoltState,
		PartyMode: bool(r.PartyMode), GuestAccessMode: bool(r.GuestAccessMode),
		TwistAssist: bool(r.TwistAssist), TouchToConnect: bool(r.TouchToConnect),
		LockDirection: r.LockDirection, MortiseLockType: r.MortiseLockType, SupportedLockStates: r.SupportedLockStates,
		BridgeIP: r.BridgeIP, BridgeHostname: r.BridgeHostname, BridgeMacWifi: r.BridgeMacWifi,
		KeySecret: r.KeySecret, BridgeKey: r.BridgeKey, BackendKey: r.BackendKey,
	}
	if l.BoltState == "" {
		l.BoltState = loqed.BoltUnknown
	}
	if r.Online != nil {
		v := bool(*r.Online)
		l.Online = &v
	}
	if r.LocalID != nil {
		v := int(*r.LocalID)
		l.LocalID = &v
	}
	return l
}

// ListLocks returns every lock the token can see. LOQED rate-limits this
// endpoint (documented: >12 requests per 12 h blocks for 12 h).
func (c *Client) ListLocks(ctx context.Context) ([]Lock, error) {
	body, err := c.get(ctx, "/api/locks/")
	if err != nil {
		return nil, err
	}
	var resp struct {
		Data *[]rawLock `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("%w: locks: %w", loqed.ErrInvalidPayload, err)
	}
	if resp.Data == nil {
		return nil, fmt.Errorf("%w: locks: missing data", loqed.ErrInvalidPayload)
	}
	locks := make([]Lock, 0, len(*resp.Data))
	for _, r := range *resp.Data {
		locks = append(locks, r.lock())
	}
	return locks, nil
}

// Command moves the bolt to open, day_lock or night_lock.
func (c *Client) Command(ctx context.Context, lockID string, s loqed.BoltState) error {
	switch s {
	case loqed.BoltOpen, loqed.BoltDayLock, loqed.BoltNightLock:
	default:
		return fmt.Errorf("cloud: unsupported bolt state %q", s)
	}
	_, err := c.get(ctx, "/api/locks/"+url.PathEscape(lockID)+"/bolt_state/"+string(s))
	return err
}

func (c *Client) get(ctx context.Context, path string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+path, nil)
	if err != nil {
		return nil, errors.New("cloud: invalid base URL or lock id")
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/json")
	body, err := transport.Do(c.hc, req)
	if err != nil {
		return nil, err
	}
	// An injected client that follows redirects lands on the HTML login
	// page with 200; never report that as success.
	if trimmed := bytes.TrimSpace(body); len(trimmed) > 0 && trimmed[0] == '<' {
		return nil, fmt.Errorf("%w: received an HTML page (login?)", loqed.ErrUnauthorized)
	}
	return body, nil
}

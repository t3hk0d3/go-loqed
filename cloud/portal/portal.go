package portal

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"regexp"
	"strings"
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/internal/transport"
)

// DefaultBaseURL is the production portal.
const DefaultBaseURL = "https://integrations.production.loqed.com"

// Client creates portal sessions.
type Client struct {
	base      string
	transport http.RoundTripper
	timeout   time.Duration
}

// Option configures a Client.
type Option func(*Client)

// WithBaseURL overrides DefaultBaseURL (for tests or proxies).
func WithBaseURL(u string) Option { return func(c *Client) { c.base = strings.TrimRight(u, "/") } }

// WithTransport sets the RoundTripper of every session's HTTP client
// (default: http.DefaultTransport). Sessions always use their own cookie jar
// and follow the portal's redirects themselves.
func WithTransport(rt http.RoundTripper) Option { return func(c *Client) { c.transport = rt } }

// New creates a portal client for DefaultBaseURL. It holds no session or
// credentials; Login starts one.
func New(opts ...Option) *Client {
	c := &Client{base: DefaultBaseURL, timeout: 20 * time.Second}
	for _, o := range opts {
		o(c)
	}
	return c
}

// Session is one logged-in browser-like session. Not safe for concurrent use.
type Session struct {
	base    string
	hc      *http.Client
	version string
	csrf    string // <meta name="csrf-token"> of the last HTML page
}

// Token is a newly created personal access token. Value is only available
// at creation time.
type Token struct {
	ID, Name string
	Value    loqed.Secret
}

// TokenInfo is a listed token (no value).
type TokenInfo struct{ ID, Name string }

type page struct {
	Component string          `json:"component"`
	Props     json.RawMessage `json:"props"`
	Version   string          `json:"version"`
}

const loginComponent = "Auth/Login"

var (
	dataPage  = regexp.MustCompile(`data-page="([^"]*)"`)
	csrfMeta  = regexp.MustCompile(`<meta\s+name="csrf-token"\s+content="([^"]*)"`)
	errNoAuth = fmt.Errorf("%w: portal session is not logged in", loqed.ErrUnauthorized)
)

// Login starts a session. Bad credentials return loqed.ErrUnauthorized.
func (c *Client) Login(ctx context.Context, email, password string) (*Session, error) {
	jar, err := cookiejar.New(nil)
	if err != nil {
		return nil, err
	}
	s := &Session{base: c.base, hc: &http.Client{Jar: jar, Timeout: c.timeout, Transport: c.transport}}
	if _, err := s.loadPage(ctx, "/login"); err != nil {
		return nil, err
	}
	p, err := s.visit(ctx, http.MethodPost, "/login", map[string]any{"email": email, "password": password, "remember": false}) // "remember me" makes the real portal fail with 500
	if err != nil {
		return nil, err
	}
	if p.Component == loginComponent || hasErrors(p.Props) {
		return nil, fmt.Errorf("%w: portal rejected the email or password", loqed.ErrUnauthorized)
	}
	return s, nil
}

// ListTokens returns the account's personal access tokens.
func (s *Session) ListTokens(ctx context.Context) ([]TokenInfo, error) {
	p, err := s.visit(ctx, http.MethodGet, "/personal-access-tokens", nil)
	if err != nil {
		return nil, err
	}
	props, err := decodeTokens(p)
	if err != nil {
		return nil, err
	}
	out := make([]TokenInfo, 0, len(props.Tokens))
	for _, t := range props.Tokens {
		out = append(out, TokenInfo{ID: string(t.ID), Name: t.Name})
	}
	return out, nil
}

// CreateToken creates a personal access token and returns its value. The
// create request is sent exactly once, whatever happens afterwards.
func (s *Session) CreateToken(ctx context.Context, name string) (Token, error) {
	before, err := s.ListTokens(ctx)
	if err != nil {
		return Token{}, err
	}
	known := make(map[string]bool, len(before))
	for _, t := range before {
		known[t.ID] = true
	}
	p, err := s.visit(ctx, http.MethodPost, "/create-personal-access-tokens", map[string]any{"name": name})
	if err != nil {
		return Token{}, err
	}
	props, err := decodeTokens(p)
	if err != nil {
		return Token{}, err
	}
	value := props.accessToken()
	if value == "" {
		return Token{}, fmt.Errorf("%w: portal did not return the new token", loqed.ErrInvalidPayload)
	}
	tok := Token{Name: name, Value: loqed.Secret(value)}
	for _, t := range props.Tokens {
		if t.Name == name && !known[string(t.ID)] {
			tok.ID = string(t.ID)
		}
	}
	return tok, nil
}

// RevokeToken deletes a personal access token.
func (s *Session) RevokeToken(ctx context.Context, id string) error {
	p, err := s.visit(ctx, http.MethodDelete, "/personal-access-tokens/"+url.PathEscape(id), nil)
	if err != nil {
		return err
	}
	if p.Component == loginComponent {
		return errNoAuth
	}
	return nil
}

// Logout ends the session.
func (s *Session) Logout(ctx context.Context) error {
	_, err := s.visit(ctx, http.MethodPost, "/logout", nil)
	return err
}

type rawToken struct {
	ID   loqed.String `json:"id"`
	Name string       `json:"name"`
}

type tokensProps struct {
	Tokens      []rawToken      `json:"tokens"`
	AccessToken json.RawMessage `json:"accessToken"`
}

func decodeTokens(p *page) (tokensProps, error) {
	if p.Component == loginComponent {
		return tokensProps{}, errNoAuth
	}
	if p.Component != "PersonalAccessTokens" {
		return tokensProps{}, fmt.Errorf("%w: unexpected portal page %q", loqed.ErrInvalidPayload, p.Component)
	}
	var tp tokensProps
	if err := json.Unmarshal(p.Props, &tp); err != nil {
		return tokensProps{}, fmt.Errorf("%w: token list: %w", loqed.ErrInvalidPayload, err)
	}
	return tp, nil
}

// accessToken accepts a plain string or an object carrying the value.
func (tp tokensProps) accessToken() string {
	var s string
	if json.Unmarshal(tp.AccessToken, &s) == nil {
		return s
	}
	var obj struct {
		AccessToken    string `json:"accessToken"`
		PlainTextToken string `json:"plainTextToken"`
	}
	if json.Unmarshal(tp.AccessToken, &obj) == nil {
		if obj.AccessToken != "" {
			return obj.AccessToken
		}
		return obj.PlainTextToken
	}
	return ""
}

func hasErrors(props json.RawMessage) bool {
	var p struct {
		Errors json.RawMessage `json:"errors"`
	}
	if json.Unmarshal(props, &p) != nil {
		return false
	}
	var m map[string]any
	return json.Unmarshal(p.Errors, &m) == nil && len(m) > 0
}

// loadPage fetches a page as HTML (a full browser visit) and returns its
// Inertia page object. It refreshes the asset version and CSRF meta token.
func (s *Session) loadPage(ctx context.Context, target string) (*page, error) {
	if !strings.HasPrefix(target, "http://") && !strings.HasPrefix(target, "https://") {
		target = s.base + target
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: bad portal location", loqed.ErrInvalidPayload)
	}
	req.Header.Set("Accept", "text/html")
	resp, body, err := transport.Send(s.hc, req)
	if err != nil {
		return nil, err
	}
	if err := transport.CheckStatus(resp.StatusCode, nil); err != nil {
		return nil, err // never keep portal HTML: it embeds the CSRF token
	}
	if m := csrfMeta.FindSubmatch(body); m != nil {
		s.csrf = html.UnescapeString(string(m[1]))
	}
	m := dataPage.FindSubmatch(body)
	if m == nil {
		return nil, fmt.Errorf("%w: portal page has no Inertia data", loqed.ErrInvalidPayload)
	}
	var p page
	if err := json.Unmarshal([]byte(html.UnescapeString(string(m[1]))), &p); err != nil {
		return nil, fmt.Errorf("%w: portal page data: %w", loqed.ErrInvalidPayload, err)
	}
	s.version = p.Version
	return &p, nil
}

// visit performs one Inertia request and returns the resulting page,
// following redirects. It never re-sends a request: on a 409 version
// conflict (which Inertia raises on the redirected GET after a mutation),
// the X-Inertia-Location page is loaded as HTML instead, which carries the
// same props, including one-time flash data such as a new token.
func (s *Session) visit(ctx context.Context, method, path string, data any) (*page, error) {
	var body io.Reader
	if data != nil {
		b, err := json.Marshal(data)
		if err != nil {
			return nil, err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, s.base+path, body)
	if err != nil {
		return nil, fmt.Errorf("%w: bad portal path", loqed.ErrInvalidPayload)
	}
	req.Header.Set("Accept", "text/html, application/xhtml+xml")
	req.Header.Set("X-Requested-With", "XMLHttpRequest")
	req.Header.Set("X-Inertia", "true")
	req.Header.Set("X-Inertia-Version", s.version)
	if data != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if s.csrf != "" {
		req.Header.Set("X-CSRF-TOKEN", s.csrf)
	}
	if tok := s.xsrfToken(); tok != "" {
		req.Header.Set("X-XSRF-TOKEN", tok)
	}
	resp, raw, err := transport.Send(s.hc, req)
	if err != nil {
		return nil, err
	}
	switch resp.StatusCode {
	case http.StatusConflict:
		loc := resp.Header.Get("X-Inertia-Location")
		if loc == "" {
			return nil, fmt.Errorf("%w: version conflict without location", loqed.ErrInvalidPayload)
		}
		return s.loadPage(ctx, loc)
	case 419:
		return nil, fmt.Errorf("%w: portal rejected the CSRF token (HTTP 419)", loqed.ErrInvalidPayload)
	}
	if err := transport.CheckStatus(resp.StatusCode, nil); err != nil {
		return nil, err
	}
	if resp.Header.Get("X-Inertia") != "true" {
		return nil, fmt.Errorf("%w: portal returned a non-Inertia response", loqed.ErrInvalidPayload)
	}
	var p page
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, fmt.Errorf("%w: portal page: %w", loqed.ErrInvalidPayload, err)
	}
	if p.Version != "" {
		s.version = p.Version
	}
	return &p, nil
}

// xsrfToken returns the decoded XSRF-TOKEN cookie, if the portal sets one
// (axios sends decodeURIComponent(cookie)).
func (s *Session) xsrfToken() string {
	u, err := url.Parse(s.base)
	if err != nil {
		return ""
	}
	for _, c := range s.hc.Jar.Cookies(u) {
		if c.Name == "XSRF-TOKEN" {
			if v, err := url.PathUnescape(c.Value); err == nil {
				return v
			}
			return c.Value
		}
	}
	return ""
}

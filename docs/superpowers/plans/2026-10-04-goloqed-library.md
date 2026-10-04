# GoLoqed Library Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build GoLoqed, a thin stateless Go client library for the LOQED local Bridge API, the cloud Lock API, and the Integrations portal (Management) API.

**Architecture:** Root package `loqed` holds shared types (errors, lenient JSON scalars, `BoltState`). `bridge`, `cloud` and `cloud/portal` are independent HTTP clients built on a small `internal/transport` helper that maps HTTP/transport failures to sentinel errors. No goroutines, timers or persistent state anywhere in the library.

**Tech Stack:** Go 1.27, standard library only (`net/http`, `crypto/hmac`, `crypto/sha256`, `encoding/json`, `net/http/cookiejar`). Tests use `testing` + `net/http/httptest` only.

**Spec:** `docs/superpowers/specs/2026-10-04-loqed-mqtt-gateway-design.md` (sections 2, 3, 4, 10)

## Global Constraints

- Module path: `github.com/t3hk0d3/go-loqed`; `go 1.27` in `go.mod`.
- Library packages (`loqed` root, `bridge`, `cloud`, `cloud/portal`, `internal/transport`) import only the standard library.
- No goroutines, timers or package-level mutable state in library packages.
- Every network method takes `context.Context` as its first parameter.
- Errors: branch only on `loqed.ErrUnauthorized`, `loqed.ErrRateLimited`, `loqed.ErrUnreachable`, `loqed.ErrBadSignature`, `loqed.ErrStaleTimestamp`, `loqed.ErrInvalidPayload`, or `*loqed.APIError`. Never put secrets, signed commands or full URLs with queries into error strings.
- Bridge request headers must be sent with the exact names `TIMESTAMP` and `HASH`.
- Bridge webhook timestamp tolerance: ±10 s.
- Cloud base URL: `https://integrations.production.loqed.com`.
- Cloud webhook decoding must never decode `key_name_admin`, `key_account_e-mail` or `key_account_name`.
- Commit messages end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

- Bridge numeric fields arriving as JSON strings (`"78"`), floats, or `null` must decode without error (Task 1 flex tests, Task 2 string-typed status test, Task 5 null `key_local_id` test).
- Command URL must be byte-identical to Python `urllib.parse.quote(base64)`: `+`→`%2B`, `=`→`%3D`, `/` kept literal (Task 3 golden test checks `RawQuery`).
- Header names `TIMESTAMP`/`HASH` must reach the wire in upper case; Go canonicalizes `Header.Set` (Task 4 round-tripper test inspects the raw header map).
- Cloud API answers unauthenticated requests with `302 → /login` (observed 2026-10-04), not 401; must surface as `ErrUnauthorized`, not a JSON decode error (Task 6 redirect test).
- Portal `XSRF-TOKEN` cookie is URL-encoded; the header value must be the decoded form, or every POST fails with 419 (Task 8 fake server rejects undecoded values).

---

## File Structure

```
go.mod                         module + go version (rewritten)
loqed.go                       package doc, BoltState
errors.go                      sentinel errors, APIError
flex.go                        lenient JSON scalars: Int, Float, Bool, String
flex_test.go, loqed_test.go
internal/transport/transport.go      Do/CheckStatus: HTTP → sentinel errors
internal/transport/transport_test.go
bridge/client.go               Client, New, options, Status
bridge/types.go                Status, Action, Triggers, Webhook
bridge/sign.go                 signCommand, encodeCommand, hashHex, be32/be64
bridge/command.go              Command
bridge/webhooks.go             List/Create/DeleteWebhook
bridge/events.go               ParseEvent + event types
bridge/*_test.go
testdata/gen_vectors.py        golden-vector generator (mirrors loqedAPI 2.1.16)
cloud/client.go                Client, New, options, ListLocks, Command, Lock
cloud/webhook.go               ParseWebhook, WebhookEvent
cloud/*_test.go
cloud/portal/portal.go         Login, Session, CreateToken/ListTokens/RevokeToken/Logout
cloud/portal/portal_test.go
cloud/portal/fake_test.go      fake Laravel/Inertia portal
```

Removed: `pkg/loqed_bridge_api/` (2023 draft), old `go.sum`.

---

### Task 1: Module reset, root package, transport helper

**Files:**
- Delete: `pkg/loqed_bridge_api/client.go`, `pkg/loqed_bridge_api/client_test.go`, `pkg/loqed_bridge_api/types.go`, `pkg/loqed_bridge_api/webhook.go`, `go.sum`
- Modify: `go.mod`, `.devcontainer/devcontainer.json`
- Create: `loqed.go`, `errors.go`, `flex.go`, `flex_test.go`, `loqed_test.go`, `internal/transport/transport.go`, `internal/transport/transport_test.go`, `.gitignore`

**Interfaces:**
- Produces:
  - `loqed.BoltState` (`string`) with constants `BoltUnknown="unknown"`, `BoltOpen="open"`, `BoltDayLock="day_lock"`, `BoltNightLock="night_lock"`; `func ParseBoltState(s string) BoltState`; `(*BoltState).UnmarshalJSON`.
  - `loqed.Int` (int64), `loqed.Float` (float64), `loqed.Bool` (bool), `loqed.String` (string) — each with lenient `UnmarshalJSON`.
  - Sentinels `loqed.ErrUnauthorized, ErrRateLimited, ErrUnreachable, ErrBadSignature, ErrStaleTimestamp, ErrInvalidPayload`; `type APIError struct{ StatusCode int; Body string }`; `func IsServerError(err error) bool`.
  - `transport.MaxBody` (1 MiB), `func transport.Do(hc *http.Client, req *http.Request) ([]byte, error)`, `func transport.CheckStatus(code int, body []byte) error`.

- [ ] **Step 1: Remove the 2023 draft and reset the module**

```bash
cd ~/dev/go-loqed
git rm -r -q pkg
rm -f go.sum
cat > go.mod <<'EOF'
module github.com/t3hk0d3/go-loqed

go 1.27
EOF
cat > .gitignore <<'EOF'
.artifact/
/loqed-mqtt
/dist/
EOF
sed -i 's#"image": "mcr.microsoft.com/devcontainers/go:1-1.21-bullseye"#"image": "mcr.microsoft.com/devcontainers/go:1"#' .devcontainer/devcontainer.json
```

- [ ] **Step 2: Write failing tests for the root package**

`flex_test.go`:

```go
package loqed_test

import (
	"encoding/json"
	"testing"

	loqed "github.com/t3hk0d3/go-loqed"
)

func TestIntDecodesNumbersStringsAndNull(t *testing.T) {
	cases := map[string]loqed.Int{
		`78`: 78, `"78"`: 78, `" 78 "`: 78, `78.0`: 78, `-1`: -1, `"-1"`: -1, `null`: 0, `""`: 0,
	}
	for in, want := range cases {
		var got struct{ V loqed.Int `json:"v"` }
		if err := json.Unmarshal([]byte(`{"v":`+in+`}`), &got); err != nil {
			t.Fatalf("%s: %v", in, err)
		}
		if got.V != want {
			t.Errorf("%s: got %d want %d", in, got.V, want)
		}
	}
}

func TestIntPointerNullIsNil(t *testing.T) {
	var got struct{ V *loqed.Int `json:"v"` }
	if err := json.Unmarshal([]byte(`{"v":null}`), &got); err != nil {
		t.Fatal(err)
	}
	if got.V != nil {
		t.Fatalf("expected nil, got %v", *got.V)
	}
}

func TestIntRejectsGarbage(t *testing.T) {
	var got struct{ V loqed.Int `json:"v"` }
	if err := json.Unmarshal([]byte(`{"v":"abc"}`), &got); err == nil {
		t.Fatal("expected error")
	}
	if err := json.Unmarshal([]byte(`{"v":{}}`), &got); err == nil {
		t.Fatal("expected error for object")
	}
}

func TestFloatBoolString(t *testing.T) {
	var got struct {
		F loqed.Float  `json:"f"`
		B loqed.Bool   `json:"b"`
		C loqed.Bool   `json:"c"`
		S loqed.String `json:"s"`
		N loqed.String `json:"n"`
	}
	in := `{"f":"10.37","b":1,"c":"false","s":"abc","n":42}`
	if err := json.Unmarshal([]byte(in), &got); err != nil {
		t.Fatal(err)
	}
	if got.F != 10.37 || got.B != true || got.C != false || got.S != "abc" || got.N != "42" {
		t.Fatalf("unexpected decode: %+v", got)
	}
}
```

`loqed_test.go`:

```go
package loqed_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	loqed "github.com/t3hk0d3/go-loqed"
)

func TestParseBoltState(t *testing.T) {
	cases := map[string]loqed.BoltState{
		"NIGHT_LOCK": loqed.BoltNightLock, "night_lock": loqed.BoltNightLock,
		"DAY_LOCK": loqed.BoltDayLock, "day_lock": loqed.BoltDayLock, "LATCH": loqed.BoltDayLock, "latch": loqed.BoltDayLock,
		"OPEN": loqed.BoltOpen, " open ": loqed.BoltOpen,
		"UNKNOWN": loqed.BoltUnknown, "": loqed.BoltUnknown, "weird": loqed.BoltUnknown,
	}
	for in, want := range cases {
		if got := loqed.ParseBoltState(in); got != want {
			t.Errorf("%q: got %q want %q", in, got, want)
		}
	}
}

func TestBoltStateUnmarshalNormalizes(t *testing.T) {
	var v struct{ S loqed.BoltState `json:"s"` }
	if err := json.Unmarshal([]byte(`{"s":"NIGHT_LOCK"}`), &v); err != nil {
		t.Fatal(err)
	}
	if v.S != loqed.BoltNightLock {
		t.Fatalf("got %q", v.S)
	}
}

func TestIsServerError(t *testing.T) {
	if !loqed.IsServerError(fmt.Errorf("wrap: %w", &loqed.APIError{StatusCode: 502})) {
		t.Error("502 should be a server error")
	}
	if loqed.IsServerError(&loqed.APIError{StatusCode: 404}) {
		t.Error("404 is not a server error")
	}
	if loqed.IsServerError(errors.New("x")) {
		t.Error("plain error is not a server error")
	}
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `go test ./...`
Expected: FAIL — `undefined: loqed.Int` (package does not exist yet).

- [ ] **Step 4: Implement the root package**

`loqed.go`:

```go
// Package loqed holds types shared by the GoLoqed clients: errors,
// lenient JSON scalars and the lock bolt state.
//
// Clients live in subpackages: bridge (local Bridge API), cloud (cloud
// Lock API) and cloud/portal (Integrations portal / Management API).
package loqed

import (
	"encoding/json"
	"strings"
)

// BoltState is the physical position of the lock bolt.
type BoltState string

const (
	BoltUnknown   BoltState = "unknown"
	BoltOpen      BoltState = "open"
	BoltDayLock   BoltState = "day_lock"
	BoltNightLock BoltState = "night_lock"
)

// ParseBoltState normalizes the spellings LOQED uses across APIs
// ("NIGHT_LOCK", "night_lock", "LATCH", "DAY_LOCK", "OPEN", ...).
// Anything unrecognized is BoltUnknown.
func ParseBoltState(s string) BoltState {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "open":
		return BoltOpen
	case "day_lock", "latch":
		return BoltDayLock
	case "night_lock":
		return BoltNightLock
	default:
		return BoltUnknown
	}
}

// UnmarshalJSON accepts any spelling understood by ParseBoltState.
func (b *BoltState) UnmarshalJSON(data []byte) error {
	var s String
	if err := s.UnmarshalJSON(data); err != nil {
		return err
	}
	*b = ParseBoltState(string(s))
	return nil
}
```

`errors.go`:

```go
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
	// ErrUnreachable: the endpoint could not be reached (dial error, timeout, reset).
	ErrUnreachable = errors.New("loqed: unreachable")
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
```

`flex.go`:

```go
package loqed

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// Int decodes a JSON number, numeric string or null (as 0).
// Use *Int to tell null/absent apart from zero.
type Int int64

func (i *Int) UnmarshalJSON(b []byte) error {
	s, null, err := scalarText(b)
	if err != nil || null {
		return err
	}
	if s == "" {
		*i = 0
		return nil
	}
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		*i = Int(n)
		return nil
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return fmt.Errorf("loqed: invalid integer %q", s)
	}
	*i = Int(f)
	return nil
}

// Float decodes a JSON number, numeric string or null (as 0).
type Float float64

func (f *Float) UnmarshalJSON(b []byte) error {
	s, null, err := scalarText(b)
	if err != nil || null {
		return err
	}
	if s == "" {
		*f = 0
		return nil
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return fmt.Errorf("loqed: invalid number %q", s)
	}
	*f = Float(v)
	return nil
}

// Bool decodes true/false, 1/0 and their string forms.
type Bool bool

func (v *Bool) UnmarshalJSON(b []byte) error {
	s, null, err := scalarText(b)
	if err != nil || null {
		return err
	}
	switch strings.ToLower(s) {
	case "1", "true":
		*v = true
	case "0", "false", "":
		*v = false
	default:
		return fmt.Errorf("loqed: invalid boolean %q", s)
	}
	return nil
}

// String decodes a JSON string, number or boolean as text.
type String string

func (v *String) UnmarshalJSON(b []byte) error {
	s, null, err := scalarText(b)
	if err != nil || null {
		return err
	}
	*v = String(s)
	return nil
}

// scalarText returns the text of a JSON scalar; null reports null=true.
func scalarText(b []byte) (text string, null bool, err error) {
	b = bytes.TrimSpace(b)
	switch {
	case len(b) == 0 || string(b) == "null":
		return "", true, nil
	case b[0] == '"':
		var s string
		if err := json.Unmarshal(b, &s); err != nil {
			return "", false, err
		}
		return strings.TrimSpace(s), false, nil
	case b[0] == '{' || b[0] == '[':
		return "", false, fmt.Errorf("loqed: expected a scalar, got %.20s", b)
	default:
		return string(b), false, nil
	}
}
```

- [ ] **Step 5: Run root tests**

Run: `go test . -v`
Expected: PASS (all 7 tests).

- [ ] **Step 6: Write failing transport tests**

`internal/transport/transport_test.go`:

```go
package transport_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/internal/transport"
)

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
	hc := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return transport.Do(hc, req)
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

func TestDoUnreachableHidesURL(t *testing.T) {
	srv := serve(t, 200, "")
	srv.Close()
	_, err := get(t, srv.URL+"/to_lock?command_signed_base64=SECRET")
	if !errors.Is(err, loqed.ErrUnreachable) {
		t.Fatalf("got %v", err)
	}
	if strings.Contains(err.Error(), "SECRET") {
		t.Fatalf("error leaks URL: %v", err)
	}
}

func TestDoCanceledContextIsNotUnreachable(t *testing.T) {
	srv := serve(t, 200, "")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
	_, err := transport.Do(http.DefaultClient, req)
	if !errors.Is(err, context.Canceled) || errors.Is(err, loqed.ErrUnreachable) {
		t.Fatalf("got %v", err)
	}
}
```

- [ ] **Step 7: Run to verify failure**

Run: `go test ./internal/transport/`
Expected: FAIL — `no non-test Go files` / `undefined: transport.Do`.

- [ ] **Step 8: Implement transport**

`internal/transport/transport.go`:

```go
// Package transport maps HTTP results onto GoLoqed's sentinel errors.
package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"

	loqed "github.com/t3hk0d3/go-loqed"
)

// MaxBody caps how much of any response is read.
const MaxBody = 1 << 20

const maxErrorBody = 256

// Do sends req and returns the body of a 2xx response. Failures map to
// loqed sentinels; the request URL is never included in errors because it
// may carry a signed command.
func Do(hc *http.Client, req *http.Request) ([]byte, error) {
	resp, err := hc.Do(req)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil, err
		}
		var ue *url.Error
		if errors.As(err, &ue) {
			err = ue.Err
		}
		return nil, fmt.Errorf("%w: %v", loqed.ErrUnreachable, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, MaxBody))
	if err != nil {
		return nil, fmt.Errorf("%w: reading response: %v", loqed.ErrUnreachable, err)
	}
	if err := CheckStatus(resp.StatusCode, body); err != nil {
		return nil, err
	}
	return body, nil
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
```

- [ ] **Step 9: Run all tests**

Run: `go test ./... && go vet ./...`
Expected: PASS, no vet findings.

- [ ] **Step 10: Commit**

```bash
git add -A
git commit -m "Reset module; add shared loqed types and transport helper

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: Bridge client and status

**Files:**
- Create: `bridge/client.go`, `bridge/types.go`, `bridge/helpers_test.go`, `bridge/status_test.go`

**Interfaces:**
- Consumes: `loqed.Int`, `loqed.Float`, `loqed.BoltState`, `loqed.ErrInvalidPayload`, `transport.Do`.
- Produces:
  - `type bridge.Credentials struct{ BridgeKey, KeySecret string; LocalKeyID uint8 }`
  - `type bridge.Option func(*Client)`; `WithHTTPClient(*http.Client)`, `WithClock(func() time.Time)`, `WithBaseURL(string)`
  - `func bridge.New(host string, creds Credentials, opts ...Option) (*Client, error)` — base URL `http://<host>`; `host` may include `:port`.
  - `func (*Client) BridgeKey() []byte`
  - `func (*Client) Status(ctx) (*Status, error)`
  - `type bridge.Status struct{ BatteryPercentage loqed.Int; BatteryType string; BatteryTypeNumeric loqed.Int; BatteryVoltage loqed.Float; BoltState loqed.BoltState; BoltStateNumeric loqed.Int; BridgeMacWifi, BridgeMacBLE string; LockOnline loqed.Int; WebhooksNumber loqed.Int; IPAddress string; UpTimestamp loqed.Int; WifiStrength, BLEStrength loqed.Int }`
  - `bridge.DefaultTimeout = 5 * time.Second`

- [ ] **Step 1: Write the test helpers and failing status tests**

`bridge/helpers_test.go`:

```go
package bridge_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/t3hk0d3/go-loqed/bridge"
)

// Golden-vector inputs; see testdata/gen_vectors.py.
const (
	testBridgeKey = "Ym9uam91ciBtb25kZQ=="
	testKeySecret = "SGFsbG8gd2VyZWxk"
)

var fixedNow = time.Unix(1700000000, 0)

func newTestClient(t *testing.T, h http.Handler) *bridge.Client {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	c, err := bridge.New("unused",
		bridge.Credentials{BridgeKey: testBridgeKey, KeySecret: testKeySecret, LocalKeyID: 1},
		bridge.WithBaseURL(srv.URL),
		bridge.WithClock(func() time.Time { return fixedNow }),
	)
	if err != nil {
		t.Fatal(err)
	}
	return c
}
```

`bridge/status_test.go`:

```go
package bridge_test

import (
	"context"
	"errors"
	"net/http"
	"testing"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/bridge"
)

// From HA core tests/components/loqed/fixtures/status_ok.json.
const statusOK = `{
  "battery_percentage": 78, "battery_type": "NICKEL_METAL_HYDRIDE", "battery_type_numeric": 1,
  "battery_voltage": 10.37, "bolt_state": "day_lock", "bolt_state_numeric": 2,
  "bridge_mac_wifi": "aa:bb:cc:dd:ee:ff", "bridge_mac_ble": "11:22:33:44:55:66",
  "lock_online": 1, "webhooks_number": 1, "ip_address": "192.168.42.12",
  "up_timestamp": 1653041994, "wifi_strength": 73, "ble_strength": 20
}`

func TestStatus(t *testing.T) {
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/status" {
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "text/html") // the bridge really does this
		_, _ = w.Write([]byte(statusOK))
	}))
	st, err := c.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if st.BatteryPercentage != 78 || st.BatteryVoltage != 10.37 || st.BoltState != loqed.BoltDayLock ||
		st.LockOnline != 1 || st.WifiStrength != 73 || st.BLEStrength != 20 || st.BridgeMacWifi != "aa:bb:cc:dd:ee:ff" {
		t.Fatalf("unexpected status: %+v", st)
	}
}

func TestStatusAcceptsStringTypedNumbers(t *testing.T) {
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"battery_percentage":"78","bolt_state":"NIGHT_LOCK","lock_online":"1","ble_strength":"-1","battery_voltage":"10.1"}`))
	}))
	st, err := c.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if st.BatteryPercentage != 78 || st.BoltState != loqed.BoltNightLock || st.LockOnline != 1 || st.BLEStrength != -1 {
		t.Fatalf("unexpected status: %+v", st)
	}
}

func TestStatusInvalidJSON(t *testing.T) {
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`<html>`))
	}))
	_, err := c.Status(context.Background())
	if !errors.Is(err, loqed.ErrInvalidPayload) {
		t.Fatalf("got %v", err)
	}
}

func TestNewRejectsBadBase64(t *testing.T) {
	if _, err := bridge.New("h", bridge.Credentials{BridgeKey: "%%%"}); err == nil {
		t.Fatal("expected error for bad bridge key")
	}
	if _, err := bridge.New("h", bridge.Credentials{KeySecret: "%%%"}); err == nil {
		t.Fatal("expected error for bad key secret")
	}
}

func TestBridgeKeyReturnsCopy(t *testing.T) {
	c, err := bridge.New("h", bridge.Credentials{BridgeKey: testBridgeKey})
	if err != nil {
		t.Fatal(err)
	}
	k := c.BridgeKey()
	k[0] ^= 0xff
	if string(c.BridgeKey()) != "bonjour monde" {
		t.Fatal("BridgeKey must return a copy")
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./bridge/`
Expected: FAIL — `undefined: bridge.New`.

- [ ] **Step 3: Implement**

`bridge/types.go`:

```go
package bridge

import loqed "github.com/t3hk0d3/go-loqed"

// Status is the bridge's GET /status document.
type Status struct {
	BatteryPercentage  loqed.Int       `json:"battery_percentage"`
	BatteryType        string          `json:"battery_type"`
	BatteryTypeNumeric loqed.Int       `json:"battery_type_numeric"`
	BatteryVoltage     loqed.Float     `json:"battery_voltage"`
	BoltState          loqed.BoltState `json:"bolt_state"`
	BoltStateNumeric   loqed.Int       `json:"bolt_state_numeric"`
	BridgeMacWifi      string          `json:"bridge_mac_wifi"`
	BridgeMacBLE       string          `json:"bridge_mac_ble"`
	LockOnline         loqed.Int       `json:"lock_online"`
	WebhooksNumber     loqed.Int       `json:"webhooks_number"`
	IPAddress          string          `json:"ip_address"`
	UpTimestamp        loqed.Int       `json:"up_timestamp"`
	WifiStrength       loqed.Int       `json:"wifi_strength"`
	BLEStrength        loqed.Int       `json:"ble_strength"`
}

// Action is a lock command understood by /to_lock.
type Action uint8

const (
	ActionOpen   Action = 1 // pull the latch
	ActionUnlock Action = 2 // day_lock
	ActionLock   Action = 3 // night_lock
)

// Triggers selects which events a bridge webhook receives.
// Bit order matches the bridge's flag bitmap.
type Triggers uint32

const (
	TriggerStateChangedOpen Triggers = 1 << iota
	TriggerStateChangedLatch
	TriggerStateChangedNightLock
	TriggerStateChangedUnknown
	TriggerGotoOpen
	TriggerGotoLatch
	TriggerGotoNightLock
	TriggerBattery
	TriggerOnlineStatus
)

// AllTriggers subscribes to every event (511).
const AllTriggers Triggers = 1<<9 - 1

func (t Triggers) bit(f Triggers) int {
	if t&f != 0 {
		return 1
	}
	return 0
}

// Webhook is a registration returned by GET /webhooks.
type Webhook struct {
	ID  loqed.Int `json:"id"`
	URL string    `json:"url"`
}
```

`bridge/client.go`:

```go
// Package bridge is a stateless client for the LOQED Bridge local HTTP API.
package bridge

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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

func WithHTTPClient(hc *http.Client) Option    { return func(c *Client) { c.hc = hc } }
func WithClock(now func() time.Time) Option   { return func(c *Client) { c.now = now } }
func WithBaseURL(base string) Option          { return func(c *Client) { c.base = strings.TrimRight(base, "/") } }

// New creates a client for the bridge at host (an IP, optionally with :port).
// Empty credentials are allowed; only Status works without them.
func New(host string, creds Credentials, opts ...Option) (*Client, error) {
	bk, err := base64.StdEncoding.DecodeString(creds.BridgeKey)
	if err != nil {
		return nil, fmt.Errorf("bridge: invalid bridge key: %w", err)
	}
	ks, err := base64.StdEncoding.DecodeString(creds.KeySecret)
	if err != nil {
		return nil, fmt.Errorf("bridge: invalid key secret: %w", err)
	}
	c := &Client{
		base:      "http://" + host,
		hc:        &http.Client{Timeout: DefaultTimeout},
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
		return nil, fmt.Errorf("%w: status: %v", loqed.ErrInvalidPayload, err)
	}
	return &st, nil
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
		return nil, fmt.Errorf("bridge: building request: %w", err)
	}
	for k, v := range header {
		req.Header[k] = []string{v}
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return transport.Do(c.hc, req)
}
```

Note: `gofmt` will realign the three `With…` one-liners; run `gofmt -w bridge/`.

- [ ] **Step 4: Run tests**

Run: `gofmt -w bridge && go test ./bridge/ -v`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add bridge
git commit -m "bridge: add client and status endpoint

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: Command signing and golden vectors

**Files:**
- Create: `bridge/sign.go`, `bridge/command.go`, `bridge/command_test.go`, `testdata/gen_vectors.py`

**Interfaces:**
- Consumes: `Client.do`, `Action`.
- Produces: `func (*Client) Command(ctx, Action) error`; unexported `signCommand(secret []byte, localID uint8, a Action, ts int64) []byte`, `encodeCommand([]byte) string`, `hashHex(parts ...[]byte) string`, `be32(uint32) []byte`, `be64(uint64) []byte` (used by Tasks 4–5).

- [ ] **Step 1: Commit the golden-vector generator**

`testdata/gen_vectors.py` (stdlib only; formulas copied from loqedAPI 2.1.16 `loqed.py`: `getcommand`, `getWebhooks`, `registerWebhook`, `deleteWebhook`, `receiveWebhook`):

```python
#!/usr/bin/env python3
"""Golden vectors for GoLoqed, mirroring loqedAPI 2.1.16 (src/loqedAPI/loqed.py).

Run: python3 testdata/gen_vectors.py
Paste the output into bridge/*_test.go when the protocol changes.
"""
import base64, hashlib, hmac, struct, urllib.parse

SECRET = "SGFsbG8gd2VyZWxk"          # key_secret
BRIDGE_KEY = "Ym9uam91ciBtb25kZQ=="  # bridge_key
KEY_ID = 1
NOW = 1700000000
K = base64.b64decode(BRIDGE_KEY)

def command(action):
    signed = struct.pack("B", 2) + struct.pack("B", 7) + NOW.to_bytes(8, "big") \
        + struct.pack("B", KEY_ID) + struct.pack("B", 1) + struct.pack("B", action)
    hm = hmac.new(base64.b64decode(SECRET), signed, hashlib.sha256).digest()
    cmd = struct.pack("Q", 0) + struct.pack("B", 2) + struct.pack("B", 7) + NOW.to_bytes(8, "big") \
        + hm + struct.pack("B", KEY_ID) + struct.pack("B", 1) + struct.pack("B", action)
    return urllib.parse.quote(base64.b64encode(cmd).decode("ascii"))

for action in (1, 2, 3):
    print(f"command action={action}: {command(action)}")
print("list webhooks:", hashlib.sha256(NOW.to_bytes(8, "big") + K).hexdigest())
url = "http://10.0.0.5:8099/webhook/lock1"
print("create webhook:", hashlib.sha256(url.encode() + (511).to_bytes(4, "big") + NOW.to_bytes(8, "big") + K).hexdigest())
print("delete webhook id=7:", hashlib.sha256((7).to_bytes(8, "big") + NOW.to_bytes(8, "big") + K).hexdigest())
body = '{"requested_state":"NIGHT_LOCK","requested_state_numeric":3,"mac_wifi":"aa","mac_ble":"bb","event_type":"STATE_CHANGED_NIGHT_LOCK","key_local_id":255}'
print("event:", hashlib.sha256(body.encode() + NOW.to_bytes(8, "big") + K).hexdigest())
```

Run: `python3 testdata/gen_vectors.py`
Expected output (these exact values are used below):

```
command action=1: AAAAAAAAAAACBwAAAABlU/EARYhOAuwxCIZ/FjVsh9UOA640EQTc6vzh2ciIqxhAvYYBAQE%3D
command action=2: AAAAAAAAAAACBwAAAABlU/EAoZ0RimZk4XA5ythE2rUqS683q6es7JEPvFp/Y443zSEBAQI%3D
command action=3: AAAAAAAAAAACBwAAAABlU/EAUTwY3s6dLYU3aJaH1s%2B%2BqSku8HERbL7hApizYmPXKfoBAQM%3D
list webhooks: 16b91b7f340c0cd4e72cf5d2405d530b4a7ca7ddc414cfee98887b9518e0598c
create webhook: 8af7db84068792277430e940898c4c367655d52e74085b9901bd3e99c1dc41a4
delete webhook id=7: 45cf08fab64cdf2595f7ff647f8893ffb4389846defa8e9e28762938bf97489d
event: a1caf7ab481834cdcb2f42ffd36e81599091485f39b76f6a88212b443107a4c7
```

- [ ] **Step 2: Write failing command tests**

`bridge/command_test.go`:

```go
package bridge_test

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/bridge"
)

func TestCommandMatchesGoldenVectors(t *testing.T) {
	golden := map[bridge.Action]string{
		bridge.ActionOpen:   "AAAAAAAAAAACBwAAAABlU/EARYhOAuwxCIZ/FjVsh9UOA640EQTc6vzh2ciIqxhAvYYBAQE%3D",
		bridge.ActionUnlock: "AAAAAAAAAAACBwAAAABlU/EAoZ0RimZk4XA5ythE2rUqS683q6es7JEPvFp/Y443zSEBAQI%3D",
		bridge.ActionLock:   "AAAAAAAAAAACBwAAAABlU/EAUTwY3s6dLYU3aJaH1s%2B%2BqSku8HERbL7hApizYmPXKfoBAQM%3D",
	}
	for action, want := range golden {
		var gotQuery string
		c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/to_lock" {
				t.Errorf("path %q", r.URL.Path)
			}
			gotQuery = r.URL.RawQuery
		}))
		if err := c.Command(context.Background(), action); err != nil {
			t.Fatal(err)
		}
		if gotQuery != "command_signed_base64="+want {
			t.Errorf("action %d:\n got  %s\n want command_signed_base64=%s", action, gotQuery, want)
		}
	}
}

func TestCommandRejectsUnknownAction(t *testing.T) {
	c := newTestClient(t, http.NotFoundHandler())
	if err := c.Command(context.Background(), bridge.Action(9)); err == nil {
		t.Fatal("expected error")
	}
}

func TestCommandErrorDoesNotLeakSignedCommand(t *testing.T) {
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	err := c.Command(context.Background(), bridge.ActionLock)
	if !errors.Is(err, loqed.ErrUnauthorized) {
		t.Fatalf("got %v", err)
	}
	if strings.Contains(err.Error(), "AAAAAAAA") {
		t.Fatalf("error leaks command: %v", err)
	}
}
```

- [ ] **Step 3: Run to verify failure**

Run: `go test ./bridge/ -run Command`
Expected: FAIL — `c.Command undefined`.

- [ ] **Step 4: Implement signing and Command**

`bridge/sign.go`:

```go
package bridge

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"strings"
)

const (
	protocolVersion = 2
	commandType     = 7
	deviceID        = 1
)

// signCommand builds the /to_lock payload (all integers big-endian):
// message_id u64=0 | protocol u8 | command_type u8 | timestamp u64 |
// HMAC-SHA256(secret, protocol|command_type|timestamp|key_id|device_id|action) |
// key_id u8 | device_id u8 | action u8
func signCommand(secret []byte, localID uint8, a Action, ts int64) []byte {
	signed := []byte{protocolVersion, commandType}
	signed = binary.BigEndian.AppendUint64(signed, uint64(ts))
	signed = append(signed, localID, deviceID, byte(a))
	mac := hmac.New(sha256.New, secret)
	mac.Write(signed)

	out := make([]byte, 8, 8+2+8+sha256.Size+3) // message_id = 0
	out = append(out, protocolVersion, commandType)
	out = binary.BigEndian.AppendUint64(out, uint64(ts))
	out = mac.Sum(out)
	return append(out, localID, deviceID, byte(a))
}

// encodeCommand matches Python's urllib.parse.quote(base64): '+' and '='
// are escaped, '/' stays literal.
var commandEscaper = strings.NewReplacer("+", "%2B", "=", "%3D")

func encodeCommand(b []byte) string {
	return commandEscaper.Replace(base64.StdEncoding.EncodeToString(b))
}

// hashHex returns hex(sha256(parts...)).
func hashHex(parts ...[]byte) string {
	h := sha256.New()
	for _, p := range parts {
		h.Write(p)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func be64(v uint64) []byte { return binary.BigEndian.AppendUint64(nil, v) }
func be32(v uint32) []byte { return binary.BigEndian.AppendUint32(nil, v) }
```

`bridge/command.go`:

```go
package bridge

import (
	"context"
	"fmt"
	"net/http"
)

// Command asks the lock to open, unlock (day_lock) or lock (night_lock).
// The bridge only acknowledges receipt; the outcome arrives as a webhook.
func (c *Client) Command(ctx context.Context, a Action) error {
	switch a {
	case ActionOpen, ActionUnlock, ActionLock:
	default:
		return fmt.Errorf("bridge: unknown action %d", a)
	}
	cmd := signCommand(c.keySecret, c.localID, a, c.now().Unix())
	_, err := c.do(ctx, http.MethodGet, "/to_lock?command_signed_base64="+encodeCommand(cmd), nil, nil)
	return err
}
```

- [ ] **Step 5: Run tests**

Run: `go test ./bridge/ -v`
Expected: PASS (8 tests).

- [ ] **Step 6: Commit**

```bash
git add bridge testdata
git commit -m "bridge: add signed lock commands with golden vectors

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: Bridge webhook management

**Files:**
- Create: `bridge/webhooks.go`, `bridge/webhooks_test.go`

**Interfaces:**
- Consumes: `hashHex`, `be32`, `be64`, `Client.do`, `Triggers`, `Webhook`.
- Produces: `func (*Client) ListWebhooks(ctx) ([]Webhook, error)`, `func (*Client) CreateWebhook(ctx, url string, t Triggers) error`, `func (*Client) DeleteWebhook(ctx, id int) error`.

- [ ] **Step 1: Write failing tests**

`bridge/webhooks_test.go`:

```go
package bridge_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/t3hk0d3/go-loqed/bridge"
)

// From HA core tests/components/loqed/fixtures/get_all_webhooks.json.
const webhooksJSON = `[{"id":1,"url":"http://10.10.10.10:8123/api/webhook/Webhook_id",
 "trigger_state_changed_open":1,"trigger_state_changed_latch":1,"trigger_state_changed_night_lock":1,
 "trigger_state_changed_unknown":1,"trigger_state_goto_open":1,"trigger_state_goto_latch":1,
 "trigger_state_goto_night_lock":1,"trigger_battery":1,"trigger_online_status":1}]`

func TestListWebhooks(t *testing.T) {
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/webhooks" {
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("Timestamp") != "1700000000" {
			t.Errorf("timestamp %q", r.Header.Get("Timestamp"))
		}
		if got := r.Header.Get("Hash"); got != "16b91b7f340c0cd4e72cf5d2405d530b4a7ca7ddc414cfee98887b9518e0598c" {
			t.Errorf("hash %q", got)
		}
		_, _ = w.Write([]byte(webhooksJSON))
	}))
	hooks, err := c.ListWebhooks(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(hooks) != 1 || hooks[0].ID != 1 || hooks[0].URL != "http://10.10.10.10:8123/api/webhook/Webhook_id" {
		t.Fatalf("got %+v", hooks)
	}
}

func TestCreateWebhook(t *testing.T) {
	const url = "http://10.0.0.5:8099/webhook/lock1"
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/webhooks" {
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Hash"); got != "8af7db84068792277430e940898c4c367655d52e74085b9901bd3e99c1dc41a4" {
			t.Errorf("hash %q", got)
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body["url"] != url {
			t.Errorf("url %v", body["url"])
		}
		for _, k := range []string{"trigger_state_changed_open", "trigger_state_changed_latch", "trigger_state_changed_night_lock",
			"trigger_state_changed_unknown", "trigger_state_goto_open", "trigger_state_goto_latch",
			"trigger_state_goto_night_lock", "trigger_battery", "trigger_online_status"} {
			if body[k] != float64(1) {
				t.Errorf("%s = %v", k, body[k])
			}
		}
	}))
	if err := c.CreateWebhook(context.Background(), url, bridge.AllTriggers); err != nil {
		t.Fatal(err)
	}
}

func TestCreateWebhookPartialTriggers(t *testing.T) {
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["trigger_battery"] != float64(1) || body["trigger_state_changed_open"] != float64(0) {
			t.Errorf("body %v", body)
		}
	}))
	if err := c.CreateWebhook(context.Background(), "http://x/y", bridge.TriggerBattery); err != nil {
		t.Fatal(err)
	}
}

func TestDeleteWebhook(t *testing.T) {
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete || r.URL.Path != "/webhooks/7" {
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Hash"); got != "45cf08fab64cdf2595f7ff647f8893ffb4389846defa8e9e28762938bf97489d" {
			t.Errorf("hash %q", got)
		}
	}))
	if err := c.DeleteWebhook(context.Background(), 7); err != nil {
		t.Fatal(err)
	}
}

type captureRT struct{ header http.Header }

func (c *captureRT) RoundTrip(r *http.Request) (*http.Response, error) {
	c.header = r.Header.Clone()
	return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("[]")), Header: http.Header{}, Request: r}, nil
}

func TestWebhookHeadersKeepUpperCaseNames(t *testing.T) {
	rt := &captureRT{}
	c, err := bridge.New("bridge", bridge.Credentials{BridgeKey: testBridgeKey},
		bridge.WithHTTPClient(&http.Client{Transport: rt}),
		bridge.WithClock(func() time.Time { return fixedNow }))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.ListWebhooks(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, ok := rt.header["TIMESTAMP"]; !ok {
		t.Errorf("TIMESTAMP header not sent verbatim: %v", rt.header)
	}
	if _, ok := rt.header["HASH"]; !ok {
		t.Errorf("HASH header not sent verbatim: %v", rt.header)
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./bridge/ -run Webhook`
Expected: FAIL — `c.ListWebhooks undefined`.

- [ ] **Step 3: Implement**

`bridge/webhooks.go`:

```go
package bridge

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	loqed "github.com/t3hk0d3/go-loqed"
)

// signedHeaders builds TIMESTAMP and HASH = hex(sha256(prefix... | ts8 | bridgeKey)).
func (c *Client) signedHeaders(prefix ...[]byte) map[string]string {
	ts := c.now().Unix()
	parts := make([][]byte, 0, len(prefix)+2)
	parts = append(parts, prefix...)
	parts = append(parts, be64(uint64(ts)), c.bridgeKey)
	return map[string]string{
		"TIMESTAMP": strconv.FormatInt(ts, 10),
		"HASH":      hashHex(parts...),
	}
}

// ListWebhooks returns all webhooks registered on the bridge.
func (c *Client) ListWebhooks(ctx context.Context) ([]Webhook, error) {
	body, err := c.do(ctx, http.MethodGet, "/webhooks", nil, c.signedHeaders())
	if err != nil {
		return nil, err
	}
	var hooks []Webhook
	if err := json.Unmarshal(body, &hooks); err != nil {
		return nil, fmt.Errorf("%w: webhooks: %v", loqed.ErrInvalidPayload, err)
	}
	return hooks, nil
}

type webhookRequest struct {
	URL                   string `json:"url"`
	StateChangedOpen      int    `json:"trigger_state_changed_open"`
	StateChangedLatch     int    `json:"trigger_state_changed_latch"`
	StateChangedNightLock int    `json:"trigger_state_changed_night_lock"`
	StateChangedUnknown   int    `json:"trigger_state_changed_unknown"`
	GotoOpen              int    `json:"trigger_state_goto_open"`
	GotoLatch             int    `json:"trigger_state_goto_latch"`
	GotoNightLock         int    `json:"trigger_state_goto_night_lock"`
	Battery               int    `json:"trigger_battery"`
	OnlineStatus          int    `json:"trigger_online_status"`
}

// CreateWebhook registers url for the selected triggers.
// HASH = sha256(url | triggers as u32 BE | ts8 | bridgeKey).
func (c *Client) CreateWebhook(ctx context.Context, url string, t Triggers) error {
	body, err := json.Marshal(webhookRequest{
		URL:                   url,
		StateChangedOpen:      t.bit(TriggerStateChangedOpen),
		StateChangedLatch:     t.bit(TriggerStateChangedLatch),
		StateChangedNightLock: t.bit(TriggerStateChangedNightLock),
		StateChangedUnknown:   t.bit(TriggerStateChangedUnknown),
		GotoOpen:              t.bit(TriggerGotoOpen),
		GotoLatch:             t.bit(TriggerGotoLatch),
		GotoNightLock:         t.bit(TriggerGotoNightLock),
		Battery:               t.bit(TriggerBattery),
		OnlineStatus:          t.bit(TriggerOnlineStatus),
	})
	if err != nil {
		return err
	}
	_, err = c.do(ctx, http.MethodPost, "/webhooks", body, c.signedHeaders([]byte(url), be32(uint32(t))))
	return err
}

// DeleteWebhook removes a registration. HASH = sha256(id as u64 BE | ts8 | bridgeKey).
func (c *Client) DeleteWebhook(ctx context.Context, id int) error {
	_, err := c.do(ctx, http.MethodDelete, "/webhooks/"+strconv.Itoa(id), nil, c.signedHeaders(be64(uint64(id))))
	return err
}
```

- [ ] **Step 4: Run tests**

Run: `go test ./bridge/ -v`
Expected: PASS (13 tests).

- [ ] **Step 5: Commit**

```bash
git add bridge
git commit -m "bridge: add signed webhook management

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: Incoming bridge webhook verification and parsing

**Files:**
- Create: `bridge/events.go`, `bridge/events_test.go`

**Interfaces:**
- Consumes: `hashHex`, `be64`, `loqed.ParseBoltState`, flex types.
- Produces:
  - `const bridge.MaxClockSkew = 10 * time.Second`
  - `func bridge.ParseEvent(bridgeKey, body []byte, hash, timestamp string, now time.Time) (Event, error)` — errors: `ErrBadSignature` (missing/malformed headers, wrong hash), `ErrStaleTimestamp` (message includes skew in seconds), `ErrInvalidPayload`.
  - `type Event interface{ isEvent() }` implemented by:
    - `StateReachedEvent{MacWifi, MacBLE, EventType string; RequestedState loqed.BoltState; KeyLocalID *int}`
    - `GoToStateEvent{MacWifi, MacBLE, EventType string; GoToState loqed.BoltState; KeyLocalID *int}`
    - `BatteryEvent{MacWifi, MacBLE, BatteryType string; BatteryPercentage int; WifiStrength, BLEStrength *int}`
    - `OnlineEvent{MacWifi, MacBLE string; WifiStrength, BLEStrength *int}`
  - `KeyLocalID` is nil when absent, null, or outside 0..255 (255 is kept).

- [ ] **Step 1: Write failing tests**

`bridge/events_test.go`:

```go
package bridge_test

import (
	"encoding/base64"
	"errors"
	"strings"
	"testing"
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/bridge"
)

const goldenEventBody = `{"requested_state":"NIGHT_LOCK","requested_state_numeric":3,"mac_wifi":"aa","mac_ble":"bb","event_type":"STATE_CHANGED_NIGHT_LOCK","key_local_id":255}`
const goldenEventHash = "a1caf7ab481834cdcb2f42ffd36e81599091485f39b76f6a88212b443107a4c7"

func key(t *testing.T) []byte {
	t.Helper()
	k, err := base64.StdEncoding.DecodeString(testBridgeKey)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func TestParseEventGoldenStateReached(t *testing.T) {
	ev, err := bridge.ParseEvent(key(t), []byte(goldenEventBody), goldenEventHash, "1700000000", fixedNow)
	if err != nil {
		t.Fatal(err)
	}
	sr, ok := ev.(bridge.StateReachedEvent)
	if !ok {
		t.Fatalf("got %T", ev)
	}
	if sr.EventType != "STATE_CHANGED_NIGHT_LOCK" || sr.RequestedState != loqed.BoltNightLock ||
		sr.KeyLocalID == nil || *sr.KeyLocalID != 255 || sr.MacWifi != "aa" || sr.MacBLE != "bb" {
		t.Fatalf("got %+v", sr)
	}
}

func TestParseEventAcceptsUpperCaseHash(t *testing.T) {
	if _, err := bridge.ParseEvent(key(t), []byte(goldenEventBody), strings.ToUpper(goldenEventHash), "1700000000", fixedNow); err != nil {
		t.Fatal(err)
	}
}

func TestParseEventSignatureErrors(t *testing.T) {
	k := key(t)
	body := []byte(goldenEventBody)
	cases := []struct {
		name, hash, ts string
		now            time.Time
		want           error
	}{
		{"missing hash", "", "1700000000", fixedNow, loqed.ErrBadSignature},
		{"missing timestamp", goldenEventHash, "", fixedNow, loqed.ErrBadSignature},
		{"malformed timestamp", goldenEventHash, "abc", fixedNow, loqed.ErrBadSignature},
		{"wrong hash", strings.Repeat("0", 64), "1700000000", fixedNow, loqed.ErrBadSignature},
		{"too old", goldenEventHash, "1700000000", fixedNow.Add(11 * time.Second), loqed.ErrStaleTimestamp},
		{"too new", goldenEventHash, "1700000000", fixedNow.Add(-11 * time.Second), loqed.ErrStaleTimestamp},
	}
	for _, c := range cases {
		_, err := bridge.ParseEvent(k, body, c.hash, c.ts, c.now)
		if !errors.Is(err, c.want) {
			t.Errorf("%s: got %v want %v", c.name, err, c.want)
		}
	}
	// Exactly at the window edge is accepted.
	if _, err := bridge.ParseEvent(k, body, goldenEventHash, "1700000000", fixedNow.Add(10*time.Second)); err != nil {
		t.Errorf("10s skew should pass: %v", err)
	}
}

func TestParseEventStaleMentionsSkew(t *testing.T) {
	_, err := bridge.ParseEvent(key(t), []byte(goldenEventBody), goldenEventHash, "1700000000", fixedNow.Add(42*time.Second))
	if err == nil || !strings.Contains(err.Error(), "42s") {
		t.Fatalf("got %v", err)
	}
}

// sign produces a valid signature for arbitrary bodies at fixedNow.
func sign(t *testing.T, body string) (string, string) {
	t.Helper()
	// Recompute exactly like the bridge: sha256(body | ts8 | key).
	h := hashForTest(append(append([]byte(body), be64ForTest(1700000000)...), key(t)...))
	return h, "1700000000"
}

func TestParseEventFamilies(t *testing.T) {
	cases := []struct {
		name string
		body string
		check func(t *testing.T, ev bridge.Event)
	}{
		{"go to state", `{"go_to_state":"DAY_LOCK","go_to_state_numeric":2,"mac_wifi":"a","mac_ble":"b","event_type":"GO_TO_STATE_TWIST_ASSIST_LATCH","key_local_id":"3"}`,
			func(t *testing.T, ev bridge.Event) {
				g := ev.(bridge.GoToStateEvent)
				if g.GoToState != loqed.BoltDayLock || g.EventType != "GO_TO_STATE_TWIST_ASSIST_LATCH" || *g.KeyLocalID != 3 {
					t.Fatalf("%+v", g)
				}
			}},
		{"state reached null key", `{"requested_state":"OPEN","event_type":"STATE_CHANGED_OPEN_REMOTE","key_local_id":null}`,
			func(t *testing.T, ev bridge.Event) {
				s := ev.(bridge.StateReachedEvent)
				if s.RequestedState != loqed.BoltOpen || s.KeyLocalID != nil {
					t.Fatalf("%+v", s)
				}
			}},
		{"motor stall", `{"requested_state":"UNKNOWN","event_type":"MOTOR_STALL"}`,
			func(t *testing.T, ev bridge.Event) {
				s := ev.(bridge.StateReachedEvent)
				if s.EventType != "MOTOR_STALL" || s.RequestedState != loqed.BoltUnknown || s.KeyLocalID != nil {
					t.Fatalf("%+v", s)
				}
			}},
		{"battery", `{"battery_type":"1","battery_percentage":"88","mac_wifi":"a","mac_ble":"b"}`,
			func(t *testing.T, ev bridge.Event) {
				b := ev.(bridge.BatteryEvent)
				if b.BatteryPercentage != 88 || b.BatteryType != "1" || b.BLEStrength != nil {
					t.Fatalf("%+v", b)
				}
			}},
		{"online", `{"wifi_strength":"-60","ble_strength":-1,"mac_wifi":"a","mac_ble":"b"}`,
			func(t *testing.T, ev bridge.Event) {
				o := ev.(bridge.OnlineEvent)
				if *o.WifiStrength != -60 || *o.BLEStrength != -1 {
					t.Fatalf("%+v", o)
				}
			}},
		{"out of range key", `{"requested_state":"OPEN","event_type":"STATE_CHANGED_OPEN","key_local_id":999}`,
			func(t *testing.T, ev bridge.Event) {
				if ev.(bridge.StateReachedEvent).KeyLocalID != nil {
					t.Fatal("expected nil key")
				}
			}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h, ts := sign(t, c.body)
			ev, err := bridge.ParseEvent(key(t), []byte(c.body), h, ts, fixedNow)
			if err != nil {
				t.Fatal(err)
			}
			c.check(t, ev)
		})
	}
}

func TestParseEventInvalidPayload(t *testing.T) {
	for _, body := range []string{`not json`, `[]`, `{"mac_wifi":"a"}`} {
		h, ts := sign(t, body)
		if _, err := bridge.ParseEvent(key(t), []byte(body), h, ts, fixedNow); !errors.Is(err, loqed.ErrInvalidPayload) {
			t.Errorf("%s: got %v", body, err)
		}
	}
}
```

Also add to `bridge/helpers_test.go` (test-only re-implementation so the tests do not depend on unexported helpers):

```go
import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
)

func hashForTest(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

func be64ForTest(v uint64) []byte { return binary.BigEndian.AppendUint64(nil, v) }
```

(Merge these imports into the existing import block of `helpers_test.go`.)

- [ ] **Step 2: Run to verify failure**

Run: `go test ./bridge/ -run ParseEvent`
Expected: FAIL — `undefined: bridge.ParseEvent`.

- [ ] **Step 3: Implement**

`bridge/events.go`:

```go
package bridge

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	loqed "github.com/t3hk0d3/go-loqed"
)

// MaxClockSkew is the accepted difference between webhook TIMESTAMP and now.
const MaxClockSkew = 10 * time.Second

// Event is a verified webhook from the bridge.
type Event interface{ isEvent() }

// StateReachedEvent: the bolt reached a state (or MOTOR_STALL).
type StateReachedEvent struct {
	MacWifi, MacBLE string
	EventType       string
	RequestedState  loqed.BoltState
	KeyLocalID      *int
}

// GoToStateEvent: the bolt started moving towards a state.
type GoToStateEvent struct {
	MacWifi, MacBLE string
	EventType       string
	GoToState       loqed.BoltState
	KeyLocalID      *int
}

// BatteryEvent: battery report. BatteryPercentage -1 means the lock is offline.
type BatteryEvent struct {
	MacWifi, MacBLE string
	BatteryType     string
	BatteryPercentage int
	WifiStrength    *int
	BLEStrength     *int
}

// OnlineEvent: signal report. BLEStrength -1 means the lock is offline.
type OnlineEvent struct {
	MacWifi, MacBLE string
	WifiStrength    *int
	BLEStrength     *int
}

func (StateReachedEvent) isEvent() {}
func (GoToStateEvent) isEvent()    {}
func (BatteryEvent) isEvent()      {}
func (OnlineEvent) isEvent()       {}

type rawEvent struct {
	MacWifi           string        `json:"mac_wifi"`
	MacBLE            string        `json:"mac_ble"`
	EventType         *string       `json:"event_type"`
	RequestedState    *loqed.String `json:"requested_state"`
	GoToState         *loqed.String `json:"go_to_state"`
	KeyLocalID        *loqed.Int    `json:"key_local_id"`
	BatteryType       *loqed.String `json:"battery_type"`
	BatteryPercentage *loqed.Int    `json:"battery_percentage"`
	WifiStrength      *loqed.Int    `json:"wifi_strength"`
	BLEStrength       *loqed.Int    `json:"ble_strength"`
}

// ParseEvent verifies and decodes a webhook POSTed by the bridge.
// hash and timestamp are the HASH and TIMESTAMP header values.
func ParseEvent(bridgeKey, body []byte, hash, timestamp string, now time.Time) (Event, error) {
	hash = strings.ToLower(strings.TrimSpace(hash))
	ts, err := strconv.ParseInt(strings.TrimSpace(timestamp), 10, 64)
	if err != nil || hash == "" {
		return nil, fmt.Errorf("%w: missing or malformed TIMESTAMP/HASH", loqed.ErrBadSignature)
	}
	skew := now.Sub(time.Unix(ts, 0))
	if skew > MaxClockSkew || skew < -MaxClockSkew {
		return nil, fmt.Errorf("%w: clock skew %s (check NTP on this host and the bridge)", loqed.ErrStaleTimestamp, skew.Round(time.Second))
	}
	want := hashHex(body, be64(uint64(ts)), bridgeKey)
	if subtle.ConstantTimeCompare([]byte(want), []byte(hash)) != 1 {
		return nil, fmt.Errorf("%w: HASH mismatch", loqed.ErrBadSignature)
	}
	var r rawEvent
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, fmt.Errorf("%w: %v", loqed.ErrInvalidPayload, err)
	}
	return classify(r)
}

func classify(r rawEvent) (Event, error) {
	switch {
	case r.EventType != nil && r.GoToState != nil:
		return GoToStateEvent{MacWifi: r.MacWifi, MacBLE: r.MacBLE, EventType: *r.EventType,
			GoToState: loqed.ParseBoltState(string(*r.GoToState)), KeyLocalID: keyID(r.KeyLocalID)}, nil
	case r.EventType != nil:
		requested := ""
		if r.RequestedState != nil {
			requested = string(*r.RequestedState)
		}
		return StateReachedEvent{MacWifi: r.MacWifi, MacBLE: r.MacBLE, EventType: *r.EventType,
			RequestedState: loqed.ParseBoltState(requested), KeyLocalID: keyID(r.KeyLocalID)}, nil
	case r.BatteryPercentage != nil:
		ev := BatteryEvent{MacWifi: r.MacWifi, MacBLE: r.MacBLE, BatteryPercentage: int(*r.BatteryPercentage),
			WifiStrength: intPtr(r.WifiStrength), BLEStrength: intPtr(r.BLEStrength)}
		if r.BatteryType != nil {
			ev.BatteryType = string(*r.BatteryType)
		}
		return ev, nil
	case r.WifiStrength != nil || r.BLEStrength != nil:
		return OnlineEvent{MacWifi: r.MacWifi, MacBLE: r.MacBLE,
			WifiStrength: intPtr(r.WifiStrength), BLEStrength: intPtr(r.BLEStrength)}, nil
	default:
		return nil, fmt.Errorf("%w: unrecognized webhook body", loqed.ErrInvalidPayload)
	}
}

func keyID(v *loqed.Int) *int {
	if v == nil || *v < 0 || *v > 255 {
		return nil
	}
	id := int(*v)
	return &id
}

func intPtr(v *loqed.Int) *int {
	if v == nil {
		return nil
	}
	i := int(*v)
	return &i
}
```

- [ ] **Step 4: Run tests**

Run: `gofmt -w bridge && go test ./bridge/ -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add bridge
git commit -m "bridge: verify and parse incoming webhooks

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: Cloud Lock API client

**Files:**
- Create: `cloud/client.go`, `cloud/client_test.go`

**Interfaces:**
- Consumes: `transport.Do`, flex types, `loqed.BoltState`.
- Produces:
  - `const cloud.DefaultBaseURL = "https://integrations.production.loqed.com"`
  - `func cloud.New(token string, opts ...Option) *Client`; `WithBaseURL(string)`, `WithHTTPClient(*http.Client)` (default client: 15 s timeout, does not follow redirects).
  - `func (*Client) ListLocks(ctx) ([]Lock, error)`
  - `func (*Client) Command(ctx, lockID string, s loqed.BoltState) error` — only `open`, `day_lock`, `night_lock`.
  - `type cloud.Lock struct{ ID, Name, ModelName string; BatteryPercentage int; BatteryType string; BoltState loqed.BoltState; PartyMode, GuestAccessMode, TwistAssist, TouchToConnect bool; LockDirection, MortiseLockType string; SupportedLockStates []string; Online *bool; BridgeIP, BridgeHostname, BridgeMacWifi string; LocalID *int; KeySecret, BridgeKey, BackendKey string }`; `func (Lock) HasLocalCredentials() bool`.

- [ ] **Step 1: Write failing tests**

`cloud/client_test.go`:

```go
package cloud_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/cloud"
)

// Based on HA core tests/components/loqed/fixtures/get_all_locks.json.
const locksJSON = `{"data":[{
  "id":"Foo","name":"MyLock","model_name":"LOQED Touch","battery_percentage":64,
  "battery_type":"nickel_metal_hydride","bolt_state":"day_lock","party_mode":false,
  "guest_access_mode":false,"twist_assist":false,"touch_to_connect":true,
  "lock_direction":"clockwise","mortise_lock_type":"cylinder_operated_no_handle_on_the_outside",
  "supported_lock_states":["open","day_lock","night_lock"],"online":true,
  "bridge_ip":"192.168.12.34","bridge_hostname":"LOQED-aabbccddeeff.local","bridge_mac_wifi":"aa:bb:cc:dd:ee:ff",
  "local_id":1,"key_secret":"SGFsbG8gd2VyZWxk","backend_key":"aGVsbG8gd29ybGQ=",
  "bridge_webhook_count":1,"bridge_key":"Ym9uam91ciBtb25kZQ=="
},{
  "id":"Bar","name":"Pure","battery_percentage":"80","bolt_state":"NIGHT_LOCK","online":0
}]}`

func newServer(t *testing.T, h http.HandlerFunc) *cloud.Client {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return cloud.New("tok", cloud.WithBaseURL(srv.URL))
}

func TestListLocks(t *testing.T) {
	c := newServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/locks/" || r.Header.Get("Authorization") != "Bearer tok" || r.Header.Get("Accept") != "application/json" {
			t.Errorf("unexpected request %s %v", r.URL.Path, r.Header)
		}
		_, _ = w.Write([]byte(locksJSON))
	})
	locks, err := c.ListLocks(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(locks) != 2 {
		t.Fatalf("got %d locks", len(locks))
	}
	a, b := locks[0], locks[1]
	if a.ID != "Foo" || a.Name != "MyLock" || a.ModelName != "LOQED Touch" || a.BoltState != loqed.BoltDayLock ||
		a.BatteryPercentage != 64 || !a.TouchToConnect || a.Online == nil || !*a.Online ||
		a.BridgeIP != "192.168.12.34" || a.LocalID == nil || *a.LocalID != 1 || a.KeySecret != "SGFsbG8gd2VyZWxk" ||
		a.BridgeKey != "Ym9uam91ciBtb25kZQ==" || a.BridgeMacWifi != "aa:bb:cc:dd:ee:ff" {
		t.Fatalf("lock a: %+v", a)
	}
	if !a.HasLocalCredentials() {
		t.Error("lock a should have local credentials")
	}
	if b.HasLocalCredentials() || b.BoltState != loqed.BoltNightLock || b.BatteryPercentage != 80 || b.Online == nil || *b.Online {
		t.Fatalf("lock b: %+v", b)
	}
}

func TestListLocksRedirectToLoginIsUnauthorized(t *testing.T) {
	c := newServer(t, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/login", http.StatusFound)
	})
	if _, err := c.ListLocks(context.Background()); !errors.Is(err, loqed.ErrUnauthorized) {
		t.Fatalf("got %v", err)
	}
}

func TestListLocksRateLimited(t *testing.T) {
	c := newServer(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusTooManyRequests) })
	if _, err := c.ListLocks(context.Background()); !errors.Is(err, loqed.ErrRateLimited) {
		t.Fatalf("got %v", err)
	}
}

func TestListLocksInvalidJSON(t *testing.T) {
	c := newServer(t, func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`<html>`)) })
	if _, err := c.ListLocks(context.Background()); !errors.Is(err, loqed.ErrInvalidPayload) {
		t.Fatalf("got %v", err)
	}
}

func TestCommand(t *testing.T) {
	var path string
	c := newServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.Header.Get("Authorization") != "Bearer tok" {
			t.Errorf("unexpected %s %v", r.Method, r.Header)
		}
		path = r.URL.EscapedPath()
	})
	if err := c.Command(context.Background(), "Yq1g/K4", loqed.BoltNightLock); err != nil {
		t.Fatal(err)
	}
	if path != "/api/locks/Yq1g%2FK4/bolt_state/night_lock" {
		t.Fatalf("path %q", path)
	}
}

func TestCommandRejectsUnknownState(t *testing.T) {
	c := cloud.New("tok")
	if err := c.Command(context.Background(), "x", loqed.BoltUnknown); err == nil {
		t.Fatal("expected error")
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./cloud/`
Expected: FAIL — `undefined: cloud.New`.

- [ ] **Step 3: Implement**

`cloud/client.go`:

```go
// Package cloud is a stateless client for the LOQED cloud Lock API
// (https://integrations.production.loqed.com/api).
package cloud

import (
	"context"
	"encoding/json"
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

func WithBaseURL(u string) Option { return func(c *Client) { c.base = strings.TrimRight(u, "/") } }

// WithHTTPClient replaces the default client. Keep redirects disabled:
// the API redirects unauthenticated calls to an HTML login page.
func WithHTTPClient(hc *http.Client) Option { return func(c *Client) { c.hc = hc } }

func New(token string, opts ...Option) *Client {
	c := &Client{
		base:  DefaultBaseURL,
		token: token,
		hc: &http.Client{
			Timeout:       15 * time.Second,
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
	return l.BridgeIP != "" && l.BridgeKey != "" && l.KeySecret != "" && l.LocalID != nil
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
		Data []rawLock `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("%w: locks: %v", loqed.ErrInvalidPayload, err)
	}
	locks := make([]Lock, 0, len(resp.Data))
	for _, r := range resp.Data {
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
		return nil, fmt.Errorf("cloud: building request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/json")
	return transport.Do(c.hc, req)
}
```

- [ ] **Step 4: Run tests**

Run: `gofmt -w cloud && go test ./cloud/ -v`
Expected: PASS (6 tests).

- [ ] **Step 5: Commit**

```bash
git add cloud
git commit -m "cloud: add Lock API client

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: Cloud webhook parsing

**Files:**
- Create: `cloud/webhook.go`, `cloud/webhook_test.go`

**Interfaces:**
- Produces:
  - `type cloud.WebhookKind int` with `KindStateReached`, `KindGoToState`, `KindSignal`, `KindOnline`.
  - `type cloud.WebhookEvent struct{ Kind WebhookKind; LockID, EventType string; RequestedState, GoToState loqed.BoltState; KeyLocalID *int; KeyNameUser string; BatteryPercentage, WifiStrength, BLEStrength *int; Online *bool }`
  - `func cloud.ParseWebhook(body []byte) (WebhookEvent, error)` — `ErrInvalidPayload` when JSON is invalid, `lock_id` is missing, or no known family matches.

- [ ] **Step 1: Write failing tests**

`cloud/webhook_test.go`:

```go
package cloud_test

import (
	"errors"
	"fmt"
	"testing"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/cloud"
)

// Payloads from the LOQED web API documentation (June 2026).
const (
	cloudStateReached = `{"requested_state":"DAY_LOCK","event_type":"STATE_CHANGED_LATCH","lock_id":"Yq1g","key_local_id":3,
		"key_name_user":"Front door","key_name_admin":"Jane Doe","key_account_e-mail":"jane@example.com","key_account_name":"Jane Doe"}`
	cloudGoTo   = `{"go_to_state":"OPEN","event_type":"GO_TO_STATE_INSTANTOPEN_OPEN","lock_id":"Yq1g","key_local_id":"3","key_name_user":"Front door"}`
	cloudSignal = `{"ble_strength":42,"wifi_strength":73,"battery_percentage":88,"lock_id":"Yq1g"}`
	cloudOnline = `{"online":1,"lock_id":"Yq1g"}`
)

func TestParseWebhookStateReached(t *testing.T) {
	ev, err := cloud.ParseWebhook([]byte(cloudStateReached))
	if err != nil {
		t.Fatal(err)
	}
	if ev.Kind != cloud.KindStateReached || ev.LockID != "Yq1g" || ev.EventType != "STATE_CHANGED_LATCH" ||
		ev.RequestedState != loqed.BoltDayLock || *ev.KeyLocalID != 3 || ev.KeyNameUser != "Front door" {
		t.Fatalf("%+v", ev)
	}
}

func TestParseWebhookNeverExposesPersonalData(t *testing.T) {
	ev, err := cloud.ParseWebhook([]byte(cloudStateReached))
	if err != nil {
		t.Fatal(err)
	}
	dump := fmt.Sprintf("%+v", ev)
	for _, leak := range []string{"jane@example.com", "Jane Doe"} {
		if contains(dump, leak) {
			t.Fatalf("event exposes %q: %s", leak, dump)
		}
	}
}

func TestParseWebhookFamilies(t *testing.T) {
	g, err := cloud.ParseWebhook([]byte(cloudGoTo))
	if err != nil || g.Kind != cloud.KindGoToState || g.GoToState != loqed.BoltOpen || *g.KeyLocalID != 3 {
		t.Fatalf("goto: %+v %v", g, err)
	}
	s, err := cloud.ParseWebhook([]byte(cloudSignal))
	if err != nil || s.Kind != cloud.KindSignal || *s.BLEStrength != 42 || *s.WifiStrength != 73 || *s.BatteryPercentage != 88 {
		t.Fatalf("signal: %+v %v", s, err)
	}
	o, err := cloud.ParseWebhook([]byte(cloudOnline))
	if err != nil || o.Kind != cloud.KindOnline || o.Online == nil || !*o.Online {
		t.Fatalf("online: %+v %v", o, err)
	}
}

func TestParseWebhookInvalid(t *testing.T) {
	for _, body := range []string{`nope`, `{"online":1}`, `{"lock_id":"x"}`} {
		if _, err := cloud.ParseWebhook([]byte(body)); !errors.Is(err, loqed.ErrInvalidPayload) {
			t.Errorf("%s: got %v", body, err)
		}
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./cloud/ -run Webhook`
Expected: FAIL — `undefined: cloud.ParseWebhook`.

- [ ] **Step 3: Implement**

`cloud/webhook.go`:

```go
package cloud

import (
	"encoding/json"
	"fmt"

	loqed "github.com/t3hk0d3/go-loqed"
)

// WebhookKind classifies outgoing cloud webhooks.
type WebhookKind int

const (
	KindStateReached WebhookKind = iota + 1
	KindGoToState
	KindSignal
	KindOnline
)

// WebhookEvent is a decoded cloud webhook. Account e-mail, account name and
// admin key name are intentionally not decoded.
type WebhookEvent struct {
	Kind              WebhookKind
	LockID            string
	EventType         string
	RequestedState    loqed.BoltState
	GoToState         loqed.BoltState
	KeyLocalID        *int
	KeyNameUser       string
	BatteryPercentage *int
	WifiStrength      *int
	BLEStrength       *int
	Online            *bool
}

type rawWebhook struct {
	LockID            loqed.String  `json:"lock_id"`
	EventType         *string       `json:"event_type"`
	RequestedState    *loqed.String `json:"requested_state"`
	GoToState         *loqed.String `json:"go_to_state"`
	KeyLocalID        *loqed.Int    `json:"key_local_id"`
	KeyNameUser       string        `json:"key_name_user"`
	BatteryPercentage *loqed.Int    `json:"battery_percentage"`
	WifiStrength      *loqed.Int    `json:"wifi_strength"`
	BLEStrength       *loqed.Int    `json:"ble_strength"`
	Online            *loqed.Bool   `json:"online"`
}

// ParseWebhook decodes an outgoing cloud webhook body. Cloud webhooks are
// unsigned; the caller must authenticate the request (path secret).
func ParseWebhook(body []byte) (WebhookEvent, error) {
	var r rawWebhook
	if err := json.Unmarshal(body, &r); err != nil {
		return WebhookEvent{}, fmt.Errorf("%w: %v", loqed.ErrInvalidPayload, err)
	}
	if r.LockID == "" {
		return WebhookEvent{}, fmt.Errorf("%w: cloud webhook without lock_id", loqed.ErrInvalidPayload)
	}
	ev := WebhookEvent{LockID: string(r.LockID), KeyNameUser: r.KeyNameUser,
		BatteryPercentage: intPtr(r.BatteryPercentage), WifiStrength: intPtr(r.WifiStrength), BLEStrength: intPtr(r.BLEStrength)}
	if r.KeyLocalID != nil && *r.KeyLocalID >= 0 && *r.KeyLocalID <= 255 {
		ev.KeyLocalID = intPtr(r.KeyLocalID)
	}
	switch {
	case r.EventType != nil && r.GoToState != nil:
		ev.Kind, ev.EventType, ev.GoToState = KindGoToState, *r.EventType, loqed.ParseBoltState(string(*r.GoToState))
	case r.EventType != nil:
		ev.Kind, ev.EventType = KindStateReached, *r.EventType
		if r.RequestedState != nil {
			ev.RequestedState = loqed.ParseBoltState(string(*r.RequestedState))
		} else {
			ev.RequestedState = loqed.BoltUnknown
		}
	case r.Online != nil:
		v := bool(*r.Online)
		ev.Kind, ev.Online = KindOnline, &v
	case r.BatteryPercentage != nil || r.WifiStrength != nil || r.BLEStrength != nil:
		ev.Kind = KindSignal
	default:
		return WebhookEvent{}, fmt.Errorf("%w: unrecognized cloud webhook", loqed.ErrInvalidPayload)
	}
	return ev, nil
}

func intPtr(v *loqed.Int) *int {
	if v == nil {
		return nil
	}
	i := int(*v)
	return &i
}
```

- [ ] **Step 4: Run tests**

Run: `go test ./cloud/ -v`
Expected: PASS (10 tests).

- [ ] **Step 5: Commit**

```bash
git add cloud
git commit -m "cloud: parse outgoing cloud webhooks without personal data

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 8: Integrations portal (Management API) client

**Files:**
- Create: `cloud/portal/portal.go`, `cloud/portal/fake_test.go`, `cloud/portal/portal_test.go`

**Interfaces:**
- Consumes: `transport.Do`, `transport.CheckStatus`, `transport.MaxBody`, `loqed.String`, sentinels.
- Produces:
  - `const portal.DefaultBaseURL = "https://integrations.production.loqed.com"`
  - `func portal.New(opts ...Option) *Client`; `WithBaseURL(string)`, `WithTransport(http.RoundTripper)`
  - `func (*Client) Login(ctx, email, password string) (*Session, error)` — `ErrUnauthorized` on rejected credentials.
  - `type portal.Token struct{ ID, Name, Value string }`, `type portal.TokenInfo struct{ ID, Name string }`
  - `func (*Session) CreateToken(ctx, name string) (Token, error)`
  - `func (*Session) ListTokens(ctx) ([]TokenInfo, error)`
  - `func (*Session) RevokeToken(ctx, id string) error`
  - `func (*Session) Logout(ctx) error`

Protocol (spec §2.3): Laravel + Inertia. `GET /login` (HTML, `data-page` attribute carries `{"version":...}`; sets `XSRF-TOKEN` + session cookies). Subsequent calls send `X-Inertia: true`, `X-Inertia-Version`, `X-Requested-With: XMLHttpRequest`, and `X-XSRF-TOKEN` = URL-decoded `XSRF-TOKEN` cookie. Redirects are followed (cookies via jar). `409` + `X-Inertia-Location` = asset version changed → reload version, retry once. `419` = CSRF/session expired → `ErrUnauthorized`.

- [ ] **Step 1: Write the fake portal**

`cloud/portal/fake_test.go`:

```go
package portal_test

import (
	"encoding/json"
	"fmt"
	"html"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
)

type fakeToken struct{ ID, Name, Value string }

// fakePortal mimics the Laravel/Inertia integrations portal closely enough
// to exercise cookies, XSRF, redirects, version mismatches and flashes.
type fakePortal struct {
	mu        sync.Mutex
	version   string
	email     string
	password  string
	sessions  map[string]*fakeSession
	tokens    []fakeToken
	nextID    int
	brokenTokensPage bool
}

type fakeSession struct {
	authed      bool
	loginErrors map[string]string
	flashToken  string
}

func newFakePortal(t *testing.T) (*fakePortal, *httptest.Server) {
	t.Helper()
	f := &fakePortal{version: "v1", email: "me@example.com", password: "s3cret", sessions: map[string]*fakeSession{}}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	return f, srv
}

func (f *fakePortal) xsrfFor(sid string) string { return "xsrf+/=" + sid } // characters that need URL-encoding

func (f *fakePortal) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	sid := ""
	if c, err := r.Cookie("laravel_session"); err == nil {
		sid = c.Value
	}
	sess, ok := f.sessions[sid]
	if !ok {
		sid = fmt.Sprintf("s%d", len(f.sessions)+1)
		sess = &fakeSession{}
		f.sessions[sid] = sess
	}
	http.SetCookie(w, &http.Cookie{Name: "laravel_session", Value: sid, Path: "/"})
	http.SetCookie(w, &http.Cookie{Name: "XSRF-TOKEN", Value: url.QueryEscape(f.xsrfFor(sid)), Path: "/"})

	if r.Method != http.MethodGet && r.Header.Get("X-XSRF-TOKEN") != f.xsrfFor(sid) {
		w.WriteHeader(419)
		return
	}
	inertia := r.Header.Get("X-Inertia") == "true"
	if inertia && r.Method == http.MethodGet && r.Header.Get("X-Inertia-Version") != f.version {
		w.Header().Set("X-Inertia-Location", r.URL.Path)
		w.WriteHeader(http.StatusConflict)
		return
	}

	switch {
	case r.URL.Path == "/login" && r.Method == http.MethodGet:
		errs := sess.loginErrors
		sess.loginErrors = nil
		if errs == nil {
			errs = map[string]string{}
		}
		f.render(w, inertia, "Auth/Login", map[string]any{"errors": errs})
	case r.URL.Path == "/login" && r.Method == http.MethodPost:
		var body struct{ Email, Password string }
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body.Email == f.email && body.Password == f.password {
			sess.authed = true
			http.Redirect(w, r, "/dashboard", http.StatusFound)
			return
		}
		sess.loginErrors = map[string]string{"email": "These credentials do not match our records."}
		http.Redirect(w, r, "/login", http.StatusFound)
	case !sess.authed:
		http.Redirect(w, r, "/login", http.StatusFound)
	case r.URL.Path == "/dashboard":
		f.render(w, inertia, "Dashboard", map[string]any{"errors": []any{}})
	case r.URL.Path == "/personal-access-tokens" && r.Method == http.MethodGet:
		list := []map[string]any{}
		for _, t := range f.tokens {
			list = append(list, map[string]any{"id": t.ID, "name": t.Name})
		}
		props := map[string]any{"errors": []any{}, "tokens": list}
		if sess.flashToken != "" {
			props["accessToken"] = sess.flashToken
			sess.flashToken = ""
		}
		component := "PersonalAccessTokens"
		if f.brokenTokensPage {
			component = "SomethingElse"
		}
		f.render(w, inertia, component, props)
	case r.URL.Path == "/create-personal-access-tokens" && r.Method == http.MethodPost:
		var body struct{ Name string }
		_ = json.NewDecoder(r.Body).Decode(&body)
		f.nextID++
		tok := fakeToken{ID: fmt.Sprintf("%040d", f.nextID), Name: body.Name, Value: fmt.Sprintf("pat-%d", f.nextID)}
		f.tokens = append(f.tokens, tok)
		sess.flashToken = tok.Value
		http.Redirect(w, r, "/personal-access-tokens", http.StatusFound)
	case strings.HasPrefix(r.URL.Path, "/personal-access-tokens/") && r.Method == http.MethodDelete:
		id := strings.TrimPrefix(r.URL.Path, "/personal-access-tokens/")
		kept := f.tokens[:0]
		for _, t := range f.tokens {
			if t.ID != id {
				kept = append(kept, t)
			}
		}
		f.tokens = kept
		http.Redirect(w, r, "/personal-access-tokens", http.StatusSeeOther)
	case r.URL.Path == "/logout" && r.Method == http.MethodPost:
		sess.authed = false
		http.Redirect(w, r, "/login", http.StatusFound)
	default:
		http.NotFound(w, r)
	}
}

func (f *fakePortal) render(w http.ResponseWriter, inertia bool, component string, props map[string]any) {
	page, _ := json.Marshal(map[string]any{"component": component, "props": props, "url": "/", "version": f.version})
	if inertia {
		w.Header().Set("X-Inertia", "true")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(page)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=UTF-8")
	fmt.Fprintf(w, `<!DOCTYPE html><html><body><div id="app" data-page="%s"></div></body></html>`, html.EscapeString(string(page)))
}

func (f *fakePortal) tokenNames() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, t := range f.tokens {
		out = append(out, t.Name)
	}
	return out
}
```

- [ ] **Step 2: Write failing tests**

`cloud/portal/portal_test.go`:

```go
package portal_test

import (
	"context"
	"errors"
	"testing"

	loqed "github.com/t3hk0d3/go-loqed"
	"github.com/t3hk0d3/go-loqed/cloud/portal"
)

func login(t *testing.T) (*fakePortal, *portal.Session) {
	t.Helper()
	f, srv := newFakePortal(t)
	s, err := portal.New(portal.WithBaseURL(srv.URL)).Login(context.Background(), "me@example.com", "s3cret")
	if err != nil {
		t.Fatal(err)
	}
	return f, s
}

func TestLoginRejectsBadPassword(t *testing.T) {
	_, srv := newFakePortal(t)
	_, err := portal.New(portal.WithBaseURL(srv.URL)).Login(context.Background(), "me@example.com", "wrong")
	if !errors.Is(err, loqed.ErrUnauthorized) {
		t.Fatalf("got %v", err)
	}
}

func TestCreateListRevokeToken(t *testing.T) {
	f, s := login(t)
	ctx := context.Background()

	tok, err := s.CreateToken(ctx, "loqed-mqtt (nas)")
	if err != nil {
		t.Fatal(err)
	}
	if tok.Value != "pat-1" || tok.Name != "loqed-mqtt (nas)" || tok.ID == "" {
		t.Fatalf("token %+v", tok)
	}

	list, err := s.ListTokens(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].ID != tok.ID || list[0].Name != tok.Name {
		t.Fatalf("list %+v", list)
	}

	if err := s.RevokeToken(ctx, tok.ID); err != nil {
		t.Fatal(err)
	}
	if names := f.tokenNames(); len(names) != 0 {
		t.Fatalf("tokens left: %v", names)
	}
	if err := s.Logout(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestCreateTokenFindsIDAmongSameNamedTokens(t *testing.T) {
	_, s := login(t)
	ctx := context.Background()
	first, err := s.CreateToken(ctx, "dup")
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.CreateToken(ctx, "dup")
	if err != nil {
		t.Fatal(err)
	}
	if first.ID == second.ID || second.Value != "pat-2" {
		t.Fatalf("first %+v second %+v", first, second)
	}
}

func TestVersionMismatchIsRetried(t *testing.T) {
	f, s := login(t)
	f.mu.Lock()
	f.version = "v2"
	f.mu.Unlock()
	if _, err := s.ListTokens(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestUnexpectedPageIsInvalidPayload(t *testing.T) {
	f, s := login(t)
	f.mu.Lock()
	f.brokenTokensPage = true
	f.mu.Unlock()
	if _, err := s.ListTokens(context.Background()); !errors.Is(err, loqed.ErrInvalidPayload) {
		t.Fatalf("got %v", err)
	}
}

func TestLoginUnreachable(t *testing.T) {
	_, err := portal.New(portal.WithBaseURL("http://127.0.0.1:1")).Login(context.Background(), "a", "b")
	if !errors.Is(err, loqed.ErrUnreachable) {
		t.Fatalf("got %v", err)
	}
}
```

- [ ] **Step 3: Run to verify failure**

Run: `go test ./cloud/portal/`
Expected: FAIL — `undefined: portal.New`.

- [ ] **Step 4: Implement**

`cloud/portal/portal.go`:

```go
// Package portal drives the LOQED Integrations portal (the undocumented
// "Management API"): log in with email/password and manage personal
// access tokens. It scrapes a Laravel + Inertia web app; any change on
// LOQED's side surfaces as loqed.ErrInvalidPayload.
package portal

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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

func WithBaseURL(u string) Option              { return func(c *Client) { c.base = strings.TrimRight(u, "/") } }
func WithTransport(rt http.RoundTripper) Option { return func(c *Client) { c.transport = rt } }

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
}

// Token is a newly created personal access token. Value is only available
// at creation time.
type Token struct{ ID, Name, Value string }

// TokenInfo is a listed token (no value).
type TokenInfo struct{ ID, Name string }

type page struct {
	Component string          `json:"component"`
	Props     json.RawMessage `json:"props"`
	Version   string          `json:"version"`
}

var dataPage = regexp.MustCompile(`data-page="([^"]*)"`)

// Login starts a session. Bad credentials return loqed.ErrUnauthorized.
func (c *Client) Login(ctx context.Context, email, password string) (*Session, error) {
	jar, err := cookiejar.New(nil)
	if err != nil {
		return nil, err
	}
	s := &Session{base: c.base, hc: &http.Client{Jar: jar, Timeout: c.timeout, Transport: c.transport}}
	if err := s.loadVersion(ctx, "/login"); err != nil {
		return nil, err
	}
	p, err := s.visit(ctx, http.MethodPost, "/login", map[string]any{"email": email, "password": password, "remember": true})
	if err != nil {
		return nil, err
	}
	if p.Component == "Auth/Login" || hasErrors(p.Props) {
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

// CreateToken creates a personal access token and returns its value.
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
	tok := Token{Name: name, Value: value}
	for _, t := range props.Tokens {
		if t.Name == name && !known[string(t.ID)] {
			tok.ID = string(t.ID)
		}
	}
	return tok, nil
}

// RevokeToken deletes a personal access token.
func (s *Session) RevokeToken(ctx context.Context, id string) error {
	_, err := s.visit(ctx, http.MethodDelete, "/personal-access-tokens/"+url.PathEscape(id), nil)
	return err
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
	if p.Component != "PersonalAccessTokens" {
		return tokensProps{}, fmt.Errorf("%w: unexpected portal page %q", loqed.ErrInvalidPayload, p.Component)
	}
	var tp tokensProps
	if err := json.Unmarshal(p.Props, &tp); err != nil {
		return tokensProps{}, fmt.Errorf("%w: token list: %v", loqed.ErrInvalidPayload, err)
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

// loadVersion fetches an HTML page and reads the Inertia asset version.
func (s *Session) loadVersion(ctx context.Context, target string) error {
	if !strings.HasPrefix(target, "http://") && !strings.HasPrefix(target, "https://") {
		target = s.base + target
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "text/html")
	body, err := transport.Do(s.hc, req)
	if err != nil {
		return err
	}
	m := dataPage.FindSubmatch(body)
	if m == nil {
		return fmt.Errorf("%w: portal page has no Inertia data", loqed.ErrInvalidPayload)
	}
	var p page
	if err := json.Unmarshal([]byte(html.UnescapeString(string(m[1]))), &p); err != nil {
		return fmt.Errorf("%w: portal page data: %v", loqed.ErrInvalidPayload, err)
	}
	s.version = p.Version
	return nil
}

// visit performs an Inertia request and returns the resulting page,
// following redirects. A 409 version conflict is retried once.
func (s *Session) visit(ctx context.Context, method, path string, data any) (*page, error) {
	for attempt := 0; attempt < 2; attempt++ {
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
			return nil, err
		}
		req.Header.Set("Accept", "text/html, application/xhtml+xml")
		req.Header.Set("X-Requested-With", "XMLHttpRequest")
		req.Header.Set("X-Inertia", "true")
		req.Header.Set("X-Inertia-Version", s.version)
		if data != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		if tok := s.xsrfToken(); tok != "" {
			req.Header.Set("X-XSRF-TOKEN", tok)
		}
		resp, err := s.hc.Do(req)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil, err
			}
			var ue *url.Error
			if errors.As(err, &ue) {
				err = ue.Err
			}
			return nil, fmt.Errorf("%w: %v", loqed.ErrUnreachable, err)
		}
		raw, err := io.ReadAll(io.LimitReader(resp.Body, transport.MaxBody))
		resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("%w: reading response: %v", loqed.ErrUnreachable, err)
		}
		switch resp.StatusCode {
		case http.StatusConflict:
			loc := resp.Header.Get("X-Inertia-Location")
			if loc == "" {
				loc = path
			}
			if err := s.loadVersion(ctx, loc); err != nil {
				return nil, err
			}
			continue
		case 419:
			return nil, fmt.Errorf("%w: portal session or XSRF token expired", loqed.ErrUnauthorized)
		}
		if err := transport.CheckStatus(resp.StatusCode, raw); err != nil {
			return nil, err
		}
		if resp.Header.Get("X-Inertia") != "true" {
			return nil, fmt.Errorf("%w: portal returned a non-Inertia response", loqed.ErrInvalidPayload)
		}
		var p page
		if err := json.Unmarshal(raw, &p); err != nil {
			return nil, fmt.Errorf("%w: portal page: %v", loqed.ErrInvalidPayload, err)
		}
		if p.Version != "" {
			s.version = p.Version
		}
		return &p, nil
	}
	return nil, fmt.Errorf("%w: portal asset version kept changing", loqed.ErrInvalidPayload)
}

// xsrfToken returns the URL-decoded XSRF-TOKEN cookie (Laravel expects the
// decoded value in X-XSRF-TOKEN).
func (s *Session) xsrfToken() string {
	u, err := url.Parse(s.base)
	if err != nil {
		return ""
	}
	for _, c := range s.hc.Jar.Cookies(u) {
		if c.Name == "XSRF-TOKEN" {
			if v, err := url.QueryUnescape(c.Value); err == nil {
				return v
			}
			return c.Value
		}
	}
	return ""
}
```

- [ ] **Step 5: Run tests**

Run: `gofmt -w cloud && go test ./cloud/... -v -race`
Expected: PASS (all portal tests plus cloud tests).

If `TestCreateListRevokeToken` fails with 419 after the POST redirect, check that the `X-XSRF-TOKEN` header is the decoded cookie value (`xsrf+/=s1`), not the raw cookie (`xsrf%2B%2F%3Ds1`).

- [ ] **Step 6: Full library check and commit**

Run: `go vet ./... && go test -race ./...`
Expected: PASS.

```bash
git add cloud
git commit -m "portal: add Integrations portal client for token management

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

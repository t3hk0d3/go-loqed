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
- Errors: branch only on `loqed.ErrUnauthorized`, `loqed.ErrRateLimited`, `loqed.ErrUnreachable` (provably not delivered), `loqed.ErrNoResponse` (may have been delivered), `loqed.ErrBadSignature`, `loqed.ErrStaleTimestamp`, `loqed.ErrInvalidPayload`, or `*loqed.APIError`. Never put secrets, signed commands, URLs (with queries), headers or portal HTML into error strings — including canceled-context and invalid-address errors. Wrap inner errors with `%w`.
- State-reached events derive the bolt state from `event_type` (`loqed.ReachedState`), never from `requested_state`.
- Code is `gofmt`-clean and passes `golangci-lint` v2.14.0 with the repo's `.golangci.yml` (created in Task 1).
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
- The real portal was observed **without** an `XSRF-TOKEN` cookie; CSRF must also work from `<meta name="csrf-token">` via `X-CSRF-TOKEN`, and a 419 must not be reported as a wrong password (Task 8 no-cookie and reject-CSRF tests).
- An Inertia 409 after a token-create redirect must not re-send the create (duplicate tokens) and must still return the flashed token (Task 8 bump-on-create test).
- A bridge that received a command but did not answer must surface as `ErrNoResponse`, never `ErrUnreachable` (the gateway would otherwise actuate the lock twice via the cloud) (Task 1 timeout/reset tests).

---

## File Structure

```
go.mod                         module + go version (rewritten)
.golangci.yml                  lint config (shared with the gateway)
loqed.go                       package doc, BoltState, ReachedState, GoToTarget
errors.go                      sentinel errors, APIError
flex.go                        lenient JSON scalars: Int, Float, Bool, String
flex_test.go, loqed_test.go
internal/transport/transport.go      Do/Send/CheckStatus: HTTP → sentinel errors
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

Removed: `pkg/loqed_bridge_api/` (2023 draft, untracked), old `go.sum`.

---

### Task 1: Module reset, root package, transport helper

**Files:**
- Delete (untracked files, so plain `rm`): `pkg/` (2023 draft), `go.sum`
- Modify: `go.mod`, `.devcontainer/devcontainer.json`
- Create: `loqed.go`, `errors.go`, `flex.go`, `flex_test.go`, `loqed_test.go`, `internal/transport/transport.go`, `internal/transport/transport_test.go`, `.gitignore`, `.golangci.yml`

**Interfaces:**
- Produces:
  - `loqed.BoltState` (`string`) with constants `BoltUnknown="unknown"`, `BoltOpen="open"`, `BoltDayLock="day_lock"`, `BoltNightLock="night_lock"`; `func ParseBoltState(s string) BoltState`; `(*BoltState).UnmarshalJSON`.
  - `func loqed.ReachedState(eventType string) (BoltState, bool /*jammed*/)`, `func loqed.IsGoToState(eventType string) bool`, `func loqed.GoToTarget(eventType, goToState string) BoltState` (used by Tasks 5 and 7, and by the gateway).
  - `loqed.Int` (int64), `loqed.Float` (float64), `loqed.Bool` (bool), `loqed.String` (string) — each with lenient `UnmarshalJSON` (`""` → zero, `null` → unchanged).
  - Sentinels `loqed.ErrUnauthorized, ErrRateLimited, ErrUnreachable, ErrNoResponse, ErrBadSignature, ErrStaleTimestamp, ErrInvalidPayload`; `type APIError struct{ StatusCode int; Body string }`; `func IsServerError(err error) bool`.
  - `transport.MaxBody` (1 MiB), `func transport.Do(hc *http.Client, req *http.Request) ([]byte, error)`, `func transport.Send(hc, req) (*http.Response, []byte, error)` (no status mapping; body read and closed), `func transport.CheckStatus(code int, body []byte) error`.

- [ ] **Step 1: Remove the 2023 draft and reset the module**

```bash
cd ~/dev/go-loqed
rm -rf pkg go.sum   # untracked 2023 draft; `git rm` would fail
test ! -e pkg
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
cat > .golangci.yml <<'YAML'
version: "2"
linters:
  default: standard
  enable:
    - errorlint
    - gosec
    - misspell
    - unconvert
  settings:
    misspell:
      ignore-rules:
        - mosquitto # the MQTT broker
  exclusions:
    presets:
      - std-error-handling
      - common-false-positives
    rules:
      # Tests use fixed fake secrets and ad-hoc servers.
      - path: _test\.go
        linters: [gosec]
      # Integer conversions are range-checked where it matters (key ids,
      # timestamps from time.Now); URLs come from configuration, not users.
      - linters: [gosec]
        text: "G115|G107|G704"
formatters:
  enable:
    - gofmt
YAML
```

Lint command used throughout both plans (no global install needed; v2.14.0 is the pinned version):

```bash
go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.14.0 run ./...
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
	// Every case starts from 99 so null (no-op) and "" (zero) are observable.
	cases := map[string]loqed.Int{
		`78`: 78, `"78"`: 78, `" 78 "`: 78, `78.0`: 78, `-1`: -1, `"-1"`: -1, `null`: 99, `""`: 0,
	}
	for in, want := range cases {
		got := struct {
			V loqed.Int `json:"v"`
		}{V: 99}
		if err := json.Unmarshal([]byte(`{"v":`+in+`}`), &got); err != nil {
			t.Fatalf("%s: %v", in, err)
		}
		if got.V != want {
			t.Errorf("%s: got %d want %d", in, got.V, want)
		}
	}
}

func TestIntPointerNullIsNil(t *testing.T) {
	var got struct {
		V *loqed.Int `json:"v"`
	}
	if err := json.Unmarshal([]byte(`{"v":null}`), &got); err != nil {
		t.Fatal(err)
	}
	if got.V != nil {
		t.Fatalf("expected nil, got %v", *got.V)
	}
}

func TestIntRejectsGarbage(t *testing.T) {
	var got struct {
		V loqed.Int `json:"v"`
	}
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

func TestReachedState(t *testing.T) {
	cases := []struct {
		in     string
		state  loqed.BoltState
		jammed bool
	}{
		{"STATE_CHANGED_OPEN", loqed.BoltOpen, false},
		{"STATE_CHANGED_OPEN_REMOTE", loqed.BoltOpen, false},
		{"STATE_CHANGED_LATCH", loqed.BoltDayLock, false},
		{"STATE_CHANGED_LATCH_REMOTE", loqed.BoltDayLock, false},
		{"STATE_CHANGED_NIGHT_LOCK", loqed.BoltNightLock, false},
		{"STATE_CHANGED_NIGHT_LOCK_REMOTE", loqed.BoltNightLock, false},
		{"STATE_CHANGED_UNKNOWN", loqed.BoltUnknown, false},
		{"MOTOR_STALL", loqed.BoltUnknown, true},
		{"SOMETHING_NEW", loqed.BoltUnknown, false},
	}
	for _, c := range cases {
		s, j := loqed.ReachedState(c.in)
		if s != c.state || j != c.jammed {
			t.Errorf("%s: got %q,%v want %q,%v", c.in, s, j, c.state, c.jammed)
		}
	}
}

func TestGoToTarget(t *testing.T) {
	cases := []struct {
		et, goTo string
		want     loqed.BoltState
	}{
		{"GO_TO_STATE_TWIST_ASSIST_LATCH", "DAY_LOCK", loqed.BoltDayLock},
		{"GO_TO_STATE_INSTANTOPEN_OPEN", "", loqed.BoltOpen},
		{"GO_TO_STATE_TOUCH_TO_LOCK", "", loqed.BoltNightLock},
		{"GO_TO_STATE_MANUAL_UNLOCK_VIA_OUTSIDE_LATCH", "", loqed.BoltDayLock},
		{"GO_TO_STATE_WHATEVER", "", loqed.BoltUnknown},
	}
	for _, c := range cases {
		if got := loqed.GoToTarget(c.et, c.goTo); got != c.want {
			t.Errorf("%s/%s: got %q want %q", c.et, c.goTo, got, c.want)
		}
	}
	if !loqed.IsGoToState("go_to_state_touch_to_lock") || loqed.IsGoToState("STATE_CHANGED_OPEN") {
		t.Error("IsGoToState")
	}
}

func TestBoltStateUnmarshalNormalizes(t *testing.T) {
	var v struct {
		S loqed.BoltState `json:"s"`
	}
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

Run: `go test .`
Expected: FAIL — `no non-test Go files in …/go-loqed` (the package does not exist yet).

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

// ReachedState derives the bolt state a "state reached" webhook reports
// from its event_type, exactly like loqedAPI (requested_state is only what
// was asked for and must not be trusted). MOTOR_STALL reports jammed with an
// unknown bolt position.
func ReachedState(eventType string) (state BoltState, jammed bool) {
	et := strings.TrimSuffix(strings.ToUpper(strings.TrimSpace(eventType)), "_REMOTE")
	switch et {
	case "STATE_CHANGED_OPEN":
		return BoltOpen, false
	case "STATE_CHANGED_LATCH":
		return BoltDayLock, false
	case "STATE_CHANGED_NIGHT_LOCK":
		return BoltNightLock, false
	case "MOTOR_STALL":
		return BoltUnknown, true
	default:
		return BoltUnknown, false
	}
}

// IsGoToState reports whether eventType announces bolt movement
// (GO_TO_STATE_*).
func IsGoToState(eventType string) bool {
	return strings.HasPrefix(strings.ToUpper(strings.TrimSpace(eventType)), "GO_TO_STATE_")
}

// GoToTarget returns the movement target: goToState when it is known,
// otherwise the suffix of the GO_TO_STATE_* event type.
func GoToTarget(eventType, goToState string) BoltState {
	if s := ParseBoltState(goToState); s != BoltUnknown {
		return s
	}
	et := strings.ToUpper(strings.TrimSpace(eventType))
	switch {
	case strings.HasSuffix(et, "_OPEN"):
		return BoltOpen
	case strings.HasSuffix(et, "_LATCH"), strings.HasSuffix(et, "_DAY_LOCK"):
		return BoltDayLock
	case strings.HasSuffix(et, "_NIGHT_LOCK"), strings.HasSuffix(et, "_TO_LOCK"):
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

// Int decodes a JSON number or numeric string; "" decodes as 0 and null
// leaves the value unchanged. Use *Int to tell null/absent apart from zero.
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

// Float decodes a JSON number or numeric string; "" decodes as 0 and null
// leaves the value unchanged.
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
Expected: PASS (9 tests).

- [ ] **Step 6: Write failing transport tests**

`internal/transport/transport_test.go`:

```go
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
```

- [ ] **Step 7: Run to verify failure**

Run: `go test ./internal/transport/`
Expected: FAIL — `no non-test Go files in …/internal/transport`.

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
```

- [ ] **Step 9: Run all tests**

Run: `gofmt -l . && go vet ./... && go test -race ./...`
Expected: `gofmt -l` prints nothing; PASS (transport: 7 tests; the timeout test takes ~1 s).

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
- Note: `bridge.New` rejects anything but `IP` or `IP:port` (no hostnames, spec §5.3); request-building errors never quote the URL.
- Produces:
  - `type bridge.Credentials struct{ BridgeKey, KeySecret string; LocalKeyID uint8 }`
  - `type bridge.Option func(*Client)`; `WithHTTPClient(*http.Client)`, `WithClock(func() time.Time)`, `WithBaseURL(string)`
  - `func bridge.New(host string, creds Credentials, opts ...Option) (*Client, error)` — base URL `http://<host>`; `host` must be an IP, optionally with `:port`.
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
	c, err := bridge.New("192.0.2.1",
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

func TestStatusMissingBoltStateIsUnknown(t *testing.T) {
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"battery_percentage":78}`))
	}))
	st, err := c.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if st.BoltState != loqed.BoltUnknown {
		t.Fatalf("bolt state %q", st.BoltState)
	}
}

func TestNewRejectsHostnames(t *testing.T) {
	for _, h := range []string{"loqed-bridge.local", "bad host:99999", "", "http://192.0.2.1"} {
		if _, err := bridge.New(h, bridge.Credentials{}); err == nil {
			t.Errorf("%q: expected error", h)
		}
	}
	for _, h := range []string{"192.0.2.1", "192.0.2.1:8080", "[2001:db8::1]:80", "2001:db8::1"} {
		if _, err := bridge.New(h, bridge.Credentials{}); err != nil {
			t.Errorf("%q: %v", h, err)
		}
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
	if _, err := bridge.New("192.0.2.1", bridge.Credentials{BridgeKey: "%%%"}); err == nil {
		t.Fatal("expected error for bad bridge key")
	}
	if _, err := bridge.New("192.0.2.1", bridge.Credentials{KeySecret: "%%%"}); err == nil {
		t.Fatal("expected error for bad key secret")
	}
}

func TestBridgeKeyReturnsCopy(t *testing.T) {
	c, err := bridge.New("192.0.2.1", bridge.Credentials{BridgeKey: testBridgeKey})
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
Expected: FAIL — `no non-test Go files in …/bridge`.

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

func WithHTTPClient(hc *http.Client) Option { return func(c *Client) { c.hc = hc } }
func WithClock(now func() time.Time) Option { return func(c *Client) { c.now = now } }
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
	return transport.Do(c.hc, req)
}
```

Note: `gofmt` will realign the three `With…` one-liners; run `gofmt -w bridge/`.

- [ ] **Step 4: Run tests**

Run: `gofmt -w bridge && go test ./bridge/ -v`
Expected: PASS (7 tests).

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
Expected: PASS (10 tests).

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
- Produces: `func (*Client) ListWebhooks(ctx) ([]Webhook, error)`, `func (*Client) CreateWebhook(ctx, url string, t Triggers) error` (masks `t` to `AllTriggers`; body JSON without HTML escaping, like Python `json.dumps`), `func (*Client) DeleteWebhook(ctx, id int) error`.

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
			t.Errorf("decode: %v", err) // t.Fatal must not run on the handler goroutine
			return
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

func TestCreateWebhookBodyMatchesPython(t *testing.T) {
	const url = "http://10.0.0.5:8099/webhook/a&b<c>"
	var raw string
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		raw = string(b)
	}))
	// Stray bits above 511 must not reach the hash or the body.
	if err := c.CreateWebhook(context.Background(), url, bridge.AllTriggers|1<<12); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(raw, `{"url":"`+url+`",`) || strings.HasSuffix(raw, "\n") {
		t.Fatalf("body %q", raw)
	}
	var hash string
	c = newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hash = r.Header.Get("Hash") }))
	if err := c.CreateWebhook(context.Background(), "http://10.0.0.5:8099/webhook/lock1", bridge.AllTriggers|1<<12); err != nil {
		t.Fatal(err)
	}
	if hash != "8af7db84068792277430e940898c4c367655d52e74085b9901bd3e99c1dc41a4" {
		t.Fatalf("stray trigger bits changed the hash: %s", hash)
	}
}

type captureRT struct{ header http.Header }

func (c *captureRT) RoundTrip(r *http.Request) (*http.Response, error) {
	c.header = r.Header.Clone()
	return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("[]")), Header: http.Header{}, Request: r}, nil
}

func TestWebhookHeadersKeepUpperCaseNames(t *testing.T) {
	rt := &captureRT{}
	c, err := bridge.New("192.0.2.1:8080", bridge.Credentials{BridgeKey: testBridgeKey},
		bridge.WithHTTPClient(&http.Client{Transport: rt}),
		bridge.WithClock(func() time.Time { return fixedNow }))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.ListWebhooks(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, ok := rt.header["TIMESTAMP"]; !ok { //nolint:staticcheck // SA1008: the bridge needs the name verbatim
		t.Errorf("TIMESTAMP header not sent verbatim: %v", rt.header)
	}
	if _, ok := rt.header["HASH"]; !ok { //nolint:staticcheck // SA1008: the bridge needs the name verbatim
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
	"bytes"
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
		return nil, fmt.Errorf("%w: webhooks: %w", loqed.ErrInvalidPayload, err)
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
	t &= AllTriggers // the body carries bits 0..8 only; the hash must agree
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false) // match Python json.dumps: '&' stays literal
	err := enc.Encode(webhookRequest{
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
	body := bytes.TrimSuffix(buf.Bytes(), []byte("\n"))
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

Run: `gofmt -l bridge && go test ./bridge/ -v`
Expected: PASS (16 tests).

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
- Consumes: `hashHex`, `be64`, `loqed.ParseBoltState`, `loqed.ReachedState`, `loqed.IsGoToState`, `loqed.GoToTarget`, flex types.
- Produces:
  - `const bridge.MaxClockSkew = 10 * time.Second`
  - `func bridge.ParseEvent(bridgeKey, body []byte, hash, timestamp string, now time.Time) (Event, error)` — checks HASH first, then skew in whole seconds (`now.Unix()-ts`, |skew| ≤ 10). Errors: `ErrBadSignature` (missing/malformed headers, wrong hash — also when the timestamp is stale), `ErrStaleTimestamp` (only with a valid hash; message includes skew in seconds), `ErrInvalidPayload`.
  - Classification: `event_type` starting with `GO_TO_STATE_` → `GoToStateEvent` (target from `go_to_state`, else from the event-type suffix); any other `event_type` → `StateReachedEvent` with `BoltState`/`Jammed` from `loqed.ReachedState(event_type)`.
  - `type Event interface{ isEvent() }` implemented by:
    - `StateReachedEvent{MacWifi, MacBLE, EventType string; BoltState loqed.BoltState; Jammed bool; RequestedState loqed.BoltState; KeyLocalID *int}` — `BoltState` is the reached state; `RequestedState` is informational only.
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
	if sr.EventType != "STATE_CHANGED_NIGHT_LOCK" || sr.BoltState != loqed.BoltNightLock || sr.Jammed ||
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
		{"wrong hash and stale", strings.Repeat("0", 64), "1700000000", fixedNow.Add(time.Hour), loqed.ErrBadSignature},
		{"negative timestamp", goldenEventHash, "-5", fixedNow, loqed.ErrBadSignature},
		{"too old", goldenEventHash, "1700000000", fixedNow.Add(11 * time.Second), loqed.ErrStaleTimestamp},
		{"too new", goldenEventHash, "1700000000", fixedNow.Add(-11 * time.Second), loqed.ErrStaleTimestamp},
	}
	for _, c := range cases {
		_, err := bridge.ParseEvent(k, body, c.hash, c.ts, c.now)
		if !errors.Is(err, c.want) {
			t.Errorf("%s: got %v want %v", c.name, err, c.want)
		}
	}
	// Whole seconds are compared, like loqedAPI: 10.9 s is still accepted.
	if _, err := bridge.ParseEvent(k, body, goldenEventHash, "1700000000", fixedNow.Add(10900*time.Millisecond)); err != nil {
		t.Errorf("10.9s skew should pass: %v", err)
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
		name  string
		body  string
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
				if s.BoltState != loqed.BoltOpen || s.KeyLocalID != nil {
					t.Fatalf("%+v", s)
				}
			}},
		// HA fixture nightlock_reached.json: requested NIGHT_LOCK but the bolt latched.
		{"reached state comes from event_type", `{"requested_state":"NIGHT_LOCK","event_type":"STATE_CHANGED_LATCH","key_local_id":1}`,
			func(t *testing.T, ev bridge.Event) {
				s := ev.(bridge.StateReachedEvent)
				if s.BoltState != loqed.BoltDayLock || s.RequestedState != loqed.BoltNightLock {
					t.Fatalf("%+v", s)
				}
			}},
		{"motor stall never reports the requested state", `{"requested_state":"NIGHT_LOCK","event_type":"MOTOR_STALL","key_local_id":2}`,
			func(t *testing.T, ev bridge.Event) {
				s := ev.(bridge.StateReachedEvent)
				if s.EventType != "MOTOR_STALL" || !s.Jammed || s.BoltState != loqed.BoltUnknown || *s.KeyLocalID != 2 {
					t.Fatalf("%+v", s)
				}
			}},
		{"go to state without go_to_state field", `{"event_type":"GO_TO_STATE_TOUCH_TO_LOCK","key_local_id":255}`,
			func(t *testing.T, ev bridge.Event) {
				g := ev.(bridge.GoToStateEvent)
				if g.GoToState != loqed.BoltNightLock || *g.KeyLocalID != 255 {
					t.Fatalf("%+v", g)
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

Replace `bridge/helpers_test.go` with this version, which adds a test-only re-implementation of the hash so the tests do not depend on unexported helpers:

`bridge/helpers_test.go`:

```go
package bridge_test

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
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
	c, err := bridge.New("192.0.2.1",
		bridge.Credentials{BridgeKey: testBridgeKey, KeySecret: testKeySecret, LocalKeyID: 1},
		bridge.WithBaseURL(srv.URL),
		bridge.WithClock(func() time.Time { return fixedNow }),
	)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func hashForTest(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

func be64ForTest(v uint64) []byte { return binary.BigEndian.AppendUint64(nil, v) }
```

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
// BoltState is derived from EventType (see loqed.ReachedState);
// RequestedState is only what was asked for.
type StateReachedEvent struct {
	MacWifi, MacBLE string
	EventType       string
	BoltState       loqed.BoltState
	Jammed          bool
	RequestedState  loqed.BoltState
	KeyLocalID      *int
}

// GoToStateEvent: the bolt started moving towards a state
// (event_type GO_TO_STATE_*).
type GoToStateEvent struct {
	MacWifi, MacBLE string
	EventType       string
	GoToState       loqed.BoltState
	KeyLocalID      *int
}

// BatteryEvent: battery report. BatteryPercentage -1 means the lock is offline.
type BatteryEvent struct {
	MacWifi, MacBLE   string
	BatteryType       string
	BatteryPercentage int
	WifiStrength      *int
	BLEStrength       *int
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
// hash and timestamp are the HASH and TIMESTAMP header values. The hash is
// checked before the timestamp, so only authentic requests can report clock
// skew. Skew is compared in whole seconds, like loqedAPI.
func ParseEvent(bridgeKey, body []byte, hash, timestamp string, now time.Time) (Event, error) {
	hash = strings.ToLower(strings.TrimSpace(hash))
	ts, err := strconv.ParseInt(strings.TrimSpace(timestamp), 10, 64)
	if err != nil || hash == "" || ts < 0 {
		return nil, fmt.Errorf("%w: missing or malformed TIMESTAMP/HASH", loqed.ErrBadSignature)
	}
	want := hashHex(body, be64(uint64(ts)), bridgeKey)
	if subtle.ConstantTimeCompare([]byte(want), []byte(hash)) != 1 {
		return nil, fmt.Errorf("%w: HASH mismatch", loqed.ErrBadSignature)
	}
	skew := now.Unix() - ts
	if skew > int64(MaxClockSkew/time.Second) || skew < -int64(MaxClockSkew/time.Second) {
		return nil, fmt.Errorf("%w: clock skew %ds (check NTP on this host and the bridge)", loqed.ErrStaleTimestamp, skew)
	}
	var r rawEvent
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, fmt.Errorf("%w: %w", loqed.ErrInvalidPayload, err)
	}
	return classify(r)
}

func classify(r rawEvent) (Event, error) {
	switch {
	case r.EventType != nil && loqed.IsGoToState(*r.EventType):
		goTo := ""
		if r.GoToState != nil {
			goTo = string(*r.GoToState)
		}
		return GoToStateEvent{MacWifi: r.MacWifi, MacBLE: r.MacBLE, EventType: *r.EventType,
			GoToState: loqed.GoToTarget(*r.EventType, goTo), KeyLocalID: keyID(r.KeyLocalID)}, nil
	case r.EventType != nil:
		requested := ""
		if r.RequestedState != nil {
			requested = string(*r.RequestedState)
		}
		state, jammed := loqed.ReachedState(*r.EventType)
		return StateReachedEvent{MacWifi: r.MacWifi, MacBLE: r.MacBLE, EventType: *r.EventType,
			BoltState: state, Jammed: jammed, RequestedState: loqed.ParseBoltState(requested),
			KeyLocalID: keyID(r.KeyLocalID)}, nil
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
Expected: PASS (22 tests).

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
  - `type cloud.Lock struct{ ID, Name, ModelName string; BatteryPercentage int; BatteryType string; BoltState loqed.BoltState; PartyMode, GuestAccessMode, TwistAssist, TouchToConnect bool; LockDirection, MortiseLockType string; SupportedLockStates []string; Online *bool; BridgeIP, BridgeHostname, BridgeMacWifi string; LocalID *int; KeySecret, BridgeKey, BackendKey string }`; `func (Lock) HasLocalCredentials() bool` (IP, both keys, and `LocalID` in 0..255).
  - A 2xx HTML body (an injected client followed the login redirect) is `ErrUnauthorized`, never success.

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
	c := newServer(t, func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`{"data":"nope"}`)) })
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
	if err := c.Command(context.Background(), "Yq1gK4oeE9KWe0ByxjX2", loqed.BoltNightLock); err != nil {
		t.Fatal(err)
	}
	if path != "/api/locks/Yq1gK4oeE9KWe0ByxjX2/bolt_state/night_lock" {
		t.Fatalf("path %q", path)
	}
}

func TestCommandFollowedRedirectToLoginIsUnauthorized(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/login" {
			_, _ = w.Write([]byte("<!DOCTYPE html><html>login</html>"))
			return
		}
		http.Redirect(w, r, "/login", http.StatusFound)
	}))
	t.Cleanup(srv.Close)
	// A caller-supplied client that follows redirects.
	c := cloud.New("tok", cloud.WithBaseURL(srv.URL), cloud.WithHTTPClient(http.DefaultClient))
	if err := c.Command(context.Background(), "x", loqed.BoltOpen); !errors.Is(err, loqed.ErrUnauthorized) {
		t.Fatalf("got %v", err)
	}
}

func TestHasLocalCredentialsRequiresValidLocalID(t *testing.T) {
	id := 256
	l := cloud.Lock{BridgeIP: "192.0.2.1", BridgeKey: "a", KeySecret: "b", LocalID: &id}
	if l.HasLocalCredentials() {
		t.Fatal("local_id 256 cannot be a key id")
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
Expected: FAIL — `no non-test Go files in …/cloud`.

- [ ] **Step 3: Implement**

`cloud/client.go`:

```go
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
		Data []rawLock `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("%w: locks: %w", loqed.ErrInvalidPayload, err)
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
```

- [ ] **Step 4: Run tests**

Run: `gofmt -w cloud && go test ./cloud/ -v`
Expected: PASS (8 tests).

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
  - `type cloud.WebhookEvent struct{ Kind WebhookKind; LockID, EventType string; BoltState loqed.BoltState; Jammed bool; RequestedState, GoToState loqed.BoltState; KeyLocalID *int; KeyNameUser string; BatteryPercentage, WifiStrength, BLEStrength *int; Online *bool }` — same classification rules as `bridge.ParseEvent` (`BoltState`/`Jammed` from `event_type`; `GO_TO_STATE_*` prefix → `KindGoToState`); `KeyLocalID` nil when outside 0..255.
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
		ev.BoltState != loqed.BoltDayLock || *ev.KeyLocalID != 3 || ev.KeyNameUser != "Front door" {
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

func TestParseWebhookStateFromEventType(t *testing.T) {
	ev, err := cloud.ParseWebhook([]byte(`{"requested_state":"NIGHT_LOCK","event_type":"MOTOR_STALL","lock_id":"x","key_local_id":999}`))
	if err != nil {
		t.Fatal(err)
	}
	if !ev.Jammed || ev.BoltState != loqed.BoltUnknown || ev.RequestedState != loqed.BoltNightLock {
		t.Fatalf("%+v", ev)
	}
	if ev.KeyLocalID != nil {
		t.Fatalf("out-of-range key id kept: %d", *ev.KeyLocalID)
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
	BoltState         loqed.BoltState // reached state, derived from EventType (KindStateReached)
	Jammed            bool            // MOTOR_STALL
	RequestedState    loqed.BoltState // raw requested_state; informational only
	GoToState         loqed.BoltState // movement target (KindGoToState)
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
		return WebhookEvent{}, fmt.Errorf("%w: %w", loqed.ErrInvalidPayload, err)
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
	case r.EventType != nil && loqed.IsGoToState(*r.EventType):
		goTo := ""
		if r.GoToState != nil {
			goTo = string(*r.GoToState)
		}
		ev.Kind, ev.EventType, ev.GoToState = KindGoToState, *r.EventType, loqed.GoToTarget(*r.EventType, goTo)
	case r.EventType != nil:
		ev.Kind, ev.EventType = KindStateReached, *r.EventType
		ev.BoltState, ev.Jammed = loqed.ReachedState(*r.EventType)
		ev.RequestedState = loqed.BoltUnknown
		if r.RequestedState != nil {
			ev.RequestedState = loqed.ParseBoltState(string(*r.RequestedState))
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
Expected: PASS (13 tests).

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
- Consumes: `transport.Send`, `transport.CheckStatus`, `loqed.String`, sentinels.
- Produces:
  - `const portal.DefaultBaseURL = "https://integrations.production.loqed.com"`
  - `func portal.New(opts ...Option) *Client`; `WithBaseURL(string)`, `WithTransport(http.RoundTripper)`
  - `func (*Client) Login(ctx, email, password string) (*Session, error)` — `ErrUnauthorized` on rejected credentials; `ErrInvalidPayload` on CSRF rejection (419) or an unrecognized page.
  - Any session call that lands on the `Auth/Login` page returns `ErrUnauthorized` (session expired).
  - `type portal.Token struct{ ID, Name, Value string }`, `type portal.TokenInfo struct{ ID, Name string }`
  - `func (*Session) CreateToken(ctx, name string) (Token, error)`
  - `func (*Session) ListTokens(ctx) ([]TokenInfo, error)`
  - `func (*Session) RevokeToken(ctx, id string) error`
  - `func (*Session) Logout(ctx) error`

Protocol (spec §2.3): Laravel + Inertia. `GET /login` returns HTML whose `data-page` attribute carries `{"version":...}` and whose `<meta name="csrf-token" content="…">` carries the CSRF token; it sets the session cookie (and maybe `XSRF-TOKEN` — the captured real response had none). Subsequent calls send `X-Inertia: true`, `X-Inertia-Version`, `X-Requested-With: XMLHttpRequest`, `X-CSRF-TOKEN` = meta token and, if the cookie exists, `X-XSRF-TOKEN` = `decodeURIComponent(cookie)` (`url.PathUnescape`, which keeps `+`). Redirects are followed (cookies via jar). Requests are **never re-sent**: Inertia version-checks only GETs, so a `409` + `X-Inertia-Location` after a mutation arrives on the redirected GET; the client then loads that location as HTML, whose props include the re-flashed data (e.g. `accessToken`). `419` = CSRF rejected → `ErrInvalidPayload` (not a password problem). Errors never include portal response bodies.

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
// to exercise cookies, CSRF, redirects, version mismatches and flashes.
type fakePortal struct {
	mu               sync.Mutex
	version          string
	email            string
	password         string
	sessions         map[string]*fakeSession
	tokens           []fakeToken
	nextID           int
	createCalls      int
	brokenTokensPage bool
	noXSRFCookie     bool // the real portal was observed without an XSRF-TOKEN cookie
	rejectCSRF       bool // answer every mutation with 419
	bumpOnCreate     bool // change the asset version while handling a create
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

// csrfFor contains characters that need URL-encoding in a cookie.
func (f *fakePortal) csrfFor(sid string) string { return "csrf+/=" + sid }

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
	if !f.noXSRFCookie {
		http.SetCookie(w, &http.Cookie{Name: "XSRF-TOKEN", Value: url.QueryEscape(f.csrfFor(sid)), Path: "/"})
	}

	// Laravel's VerifyCsrfToken accepts X-CSRF-TOKEN (meta) or X-XSRF-TOKEN (cookie).
	if r.Method != http.MethodGet {
		valid := r.Header.Get("X-CSRF-TOKEN") == f.csrfFor(sid) || r.Header.Get("X-XSRF-TOKEN") == f.csrfFor(sid)
		if f.rejectCSRF || !valid {
			w.WriteHeader(419)
			return
		}
	}
	inertia := r.Header.Get("X-Inertia") == "true"
	if inertia && r.Method == http.MethodGet && r.Header.Get("X-Inertia-Version") != f.version {
		// Real Inertia reflashes session data, so the flash survives the 409.
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
		f.render(w, sid, inertia, "Auth/Login", map[string]any{"errors": errs})
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
		f.render(w, sid, inertia, "Dashboard", map[string]any{"errors": []any{}})
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
		f.render(w, sid, inertia, component, props)
	case r.URL.Path == "/create-personal-access-tokens" && r.Method == http.MethodPost:
		var body struct{ Name string }
		_ = json.NewDecoder(r.Body).Decode(&body)
		f.createCalls++
		f.nextID++
		tok := fakeToken{ID: fmt.Sprintf("%040d", f.nextID), Name: body.Name, Value: fmt.Sprintf("pat-%d", f.nextID)}
		f.tokens = append(f.tokens, tok)
		sess.flashToken = tok.Value
		if f.bumpOnCreate {
			f.version += "-new"
		}
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

func (f *fakePortal) render(w http.ResponseWriter, sid string, inertia bool, component string, props map[string]any) {
	page, _ := json.Marshal(map[string]any{"component": component, "props": props, "url": "/", "version": f.version})
	if inertia {
		w.Header().Set("X-Inertia", "true")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(page)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=UTF-8")
	_, _ = fmt.Fprintf(w, `<!DOCTYPE html><html><head><meta name="csrf-token" content="%s"></head>`+
		`<body><div id="app" data-page="%s"></div></body></html>`,
		html.EscapeString(f.csrfFor(sid)), html.EscapeString(string(page)))
}

func (f *fakePortal) set(fn func(*fakePortal)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn(f)
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

func login(t *testing.T, setup ...func(*fakePortal)) (*fakePortal, *portal.Session) {
	t.Helper()
	f, srv := newFakePortal(t)
	for _, fn := range setup {
		f.set(fn)
	}
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

func TestLoginWithoutXSRFCookieUsesMetaToken(t *testing.T) {
	_, s := login(t, func(f *fakePortal) { f.noXSRFCookie = true })
	if _, err := s.CreateToken(context.Background(), "x"); err != nil {
		t.Fatal(err)
	}
}

func TestCSRFRejectionIsNotReportedAsBadPassword(t *testing.T) {
	f, srv := newFakePortal(t)
	f.set(func(f *fakePortal) { f.rejectCSRF = true })
	_, err := portal.New(portal.WithBaseURL(srv.URL)).Login(context.Background(), "me@example.com", "s3cret")
	if !errors.Is(err, loqed.ErrInvalidPayload) || errors.Is(err, loqed.ErrUnauthorized) {
		t.Fatalf("got %v", err)
	}
}

func TestCreateListRevokeToken(t *testing.T) {
	f, s := login(t)
	ctx := context.Background()

	tok, err := s.CreateToken(ctx, "loqed-mqtt 1a2b3c4d")
	if err != nil {
		t.Fatal(err)
	}
	if tok.Value != "pat-1" || tok.Name != "loqed-mqtt 1a2b3c4d" || tok.ID == "" {
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

func TestVersionMismatchOnGetLoadsPage(t *testing.T) {
	f, s := login(t)
	f.set(func(f *fakePortal) { f.version = "v2" })
	if _, err := s.ListTokens(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// Inertia version-checks the GET that follows the create redirect. The
// create must not be re-sent and the flashed token must not be lost.
func TestVersionMismatchAfterCreateKeepsTokenAndDoesNotResend(t *testing.T) {
	f, s := login(t, func(f *fakePortal) { f.bumpOnCreate = true })
	tok, err := s.CreateToken(context.Background(), "once")
	if err != nil {
		t.Fatal(err)
	}
	if tok.Value != "pat-1" || tok.ID == "" {
		t.Fatalf("token %+v", tok)
	}
	f.mu.Lock()
	calls := f.createCalls
	f.mu.Unlock()
	if calls != 1 {
		t.Fatalf("create sent %d times", calls)
	}
}

func TestExpiredSessionIsUnauthorized(t *testing.T) {
	f, s := login(t)
	f.set(func(f *fakePortal) {
		for _, sess := range f.sessions {
			sess.authed = false
		}
	})
	if _, err := s.ListTokens(context.Background()); !errors.Is(err, loqed.ErrUnauthorized) {
		t.Fatalf("got %v", err)
	}
}

func TestUnexpectedPageIsInvalidPayload(t *testing.T) {
	f, s := login(t)
	f.set(func(f *fakePortal) { f.brokenTokensPage = true })
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
Expected: FAIL — `no non-test Go files in …/cloud/portal`.

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

func WithBaseURL(u string) Option               { return func(c *Client) { c.base = strings.TrimRight(u, "/") } }
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
	csrf    string // <meta name="csrf-token"> of the last HTML page
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
	p, err := s.visit(ctx, http.MethodPost, "/login", map[string]any{"email": email, "password": password, "remember": true})
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
```

- [ ] **Step 5: Run tests**

Run: `gofmt -w cloud && go test ./cloud/... -v -race`
Expected: PASS (10 portal tests plus 13 cloud tests).

If `TestCreateListRevokeToken` fails with 419, check that `X-XSRF-TOKEN` is the decoded cookie value (`csrf+/=s1`, via `url.PathUnescape`), not the raw cookie (`csrf%2B%2F%3Ds1`), and that `X-CSRF-TOKEN` carries the meta value.

- [ ] **Step 6: Full library check and commit**

Run:

```bash
gofmt -l . && go vet ./... && go test -race ./...
go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.14.0 run ./...
python3 testdata/gen_vectors.py   # must still print the values pasted in Task 3
```

Expected: `gofmt -l` prints nothing; all packages PASS; lint reports `0 issues.`

```bash
git add cloud
git commit -m "portal: add Integrations portal client for token management

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

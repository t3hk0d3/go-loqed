# go-loqed

Monorepo with two Go modules that share one version tag (`vX.Y.Z`, tagged by the release workflow):

- **GoLoqed** — module `github.com/t3hk0d3/go-loqed` at the repo root, **Go 1.22+, standard library only**. Stateless client library: `bridge` (local Bridge API), `cloud` (cloud Lock API + cloud webhook parsing), `cloud/portal` (Integrations portal: login, mint/list/revoke tokens). Root package `loqed` holds shared types and error sentinels. Package docs (`doc.go`) and runnable examples (`example_test.go`) are its user documentation.
- **loqed-mqtt** — module `github.com/t3hk0d3/go-loqed/loqed-mqtt` in `loqed-mqtt/`, Go 1.27: local-first MQTT gateway with cloud fallback and Home Assistant discovery (`loqed-mqtt/cmd/loqed-mqtt`). Its `go.mod` has `replace github.com/t3hk0d3/go-loqed => ../`, so it always builds against the library in this checkout. Ships as a Docker image (built from the repo root) and an HA add-on (`addon/`, stays at the root).

There is no committed `go.work` (it is ignored): `./...` stops at the module boundary, so every command runs once per module.

The spec is the source of truth for behaviour and every config setting:
`docs/superpowers/specs/2026-10-04-loqed-mqtt-gateway-design.md`. Plans (with Global Constraints and Review Focus) live in `docs/superpowers/plans/`.

## Commands

Run each check in **both** modules: at the root (library) and in `loqed-mqtt/` (gateway). CI does exactly this.

```sh
go build ./...
go test -race ./...                         # full suite of the current module
gofmt -l .                                  # must print nothing
go vet ./...
go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.14.0 run ./...   # pinned; the root .golangci.yml serves both modules
go test ./... -run Example -v               # library examples (root)
GOTOOLCHAIN=go1.22.0 go test ./...          # library on its minimum Go version (root; CI job library-min-go)
test "$(go list -m all)" = github.com/t3hk0d3/go-loqed   # library has no dependencies (root)
python3 testdata/gen_vectors.py             # regenerate bridge signing golden vectors (root)

cd loqed-mqtt
go test ./internal/gateway -run TestName    # single test
go test ./internal/mqtt/hass -run TestDiscoveryPayload -update   # rewrite HA discovery golden file (review the diff)
go run ./cmd/loqed-mqtt healthcheck         # exit 0 when /healthz answers

docker build --target standalone .          # from the repo root; or --target addon
```

Before calling work done: `gofmt`, `go vet`, `go test -race ./...` and golangci-lint must all be clean in both modules.

## Layout and dependency rules

```
go.mod                          library module (no requires, go 1.22)
loqed.go errors.go flex.go      root package: shared types, error sentinels, lenient JSON numbers (doc.go: package docs)
bridge/                         local bridge client (HMAC-signed commands, webhooks, status)
cloud/  cloud/portal/           cloud Lock API, cloud webhooks, portal (Laravel/Inertia scraping)
internal/transport/             HTTP Do/Send with ErrUnreachable vs ErrNoResponse classification
testdata/gen_vectors.py         bridge signing golden vectors (pasted into bridge/*_test.go)
loqed-mqtt/go.mod               gateway module (go 1.27, paho/mochi/yaml, replace => ../)
loqed-mqtt/cmd/loqed-mqtt/      main: wiring only (+ `healthcheck` subcommand)
loqed-mqtt/internal/app/        top-level assembly and lifecycle
loqed-mqtt/internal/config/     config load/validate (env > YAML > /data/options.json), Supervisor MQTT lookup
loqed-mqtt/internal/auth/       cloud auth: token or email/password → minted token
loqed-mqtt/internal/store/      credential cache /data/locks.json (atomic write, 0600)
loqed-mqtt/internal/gateway/    per-lock supervisors, local/cloud/offline modes, failover, cloud budget
loqed-mqtt/internal/webhook/    HTTP listener for bridge/cloud webhooks, /healthz
loqed-mqtt/internal/mqtt/       MQTT client, <base>/... topics, state/event/command_status publishing, command input
loqed-mqtt/internal/mqtt/hass/  Home Assistant discovery (documents, discovery topic, HA birth topic; golden file in testdata/)
loqed-mqtt/internal/model/      normalized lock state/event model and mapping
loqed-mqtt/internal/testutil/   in-process MQTT broker (mochi) for tests
addon/  repository.yaml         HA add-on config.yaml, DOCS.md, translations (must stay at the repo root)
Dockerfile                      builds loqed-mqtt/ with the repo root as context
```

Below, `internal/...` gateway paths are relative to `loqed-mqtt/`.

- Library packages (`loqed`, `bridge`, `cloud`, `cloud/portal`, `internal/transport`) import **only the standard library**: the root `go.mod` has **no `require`s**, and CI fails if `go list -m all` shows any module besides the library. Keep the root `go` directive at the oldest Go version the library really supports (currently 1.22; CI job `library-min-go` tests it), so no newer stdlib APIs or language features in library code, examples or tests. Library packages have **no goroutines, timers or package-level mutable state**, every network method takes `context.Context` first, and they never import gateway packages.
- The library's API is public: changing it is a user-visible change (a CHANGELOG `Library:` line), every exported identifier needs godoc, and `doc.go` and the examples stay in step.
- `internal/gateway` is the only package that knows local vs cloud. `internal/mqtt` is the only package that imports the MQTT library (besides the test helper `internal/testutil`) and knows nothing about Home Assistant; `internal/mqtt/hass` holds everything HA specific and reaches the client only through the `mqtt.Discovery` interface (never the reverse; `internal/mqtt/boundary_test.go` enforces both). Gateway and MQTT talk through a small interface plus a command channel.
- Bridges are addressed by IP only — never hostnames/mDNS.

## Conventions

- **Style:** `gofmt` + `.golangci.yml` (standard linters + errorlint, gosec, misspell, unconvert). Follow the surrounding code; keep comments sparse and explanatory.
- **Errors:** callers branch only on `loqed.ErrUnauthorized`, `ErrRateLimited`, `ErrUnreachable` (provably not delivered), `ErrNoResponse` (may have been delivered), `ErrBadSignature`, `ErrStaleTimestamp`, `ErrInvalidPayload`, or `*loqed.APIError` (plus `cloud.ErrKeyDeleted` for cloud commands). Wrap with `%w`; use `errors.Is/As`.
- **No secrets in errors or logs:** never include tokens, passwords, keys, signed commands, URLs/query strings, headers, portal HTML, full cloud responses or cloud webhook bodies — this includes context-canceled and invalid-address errors. Cloud webhook decoding must never decode `key_name_admin`, `key_account_email`/`key_account_e-mail`, `key_account_name` or `value1..value3` (they carry the account e-mail).
- **Logging:** `log/slog`; mode transitions at info; repeated identical warnings are rate-limited.
- **Tests:** stdlib `testing` only, table tests against `httptest` servers, fixtures/golden files under the package's `testdata/`. Library examples (`Example*` in `example_test.go`, package `xxx_test`) use `httptest` fakes and have `// Output:`. Test names read as behaviour (`TestCommandErrorDoesNotLeakSignedCommand`). Gateway tests use fake bridge/cloud interfaces and an injectable clock — no real sleeps. Integration tests use `internal/testutil` (in-process broker).
- **Changelog:** every user-visible change adds a line under `## [Unreleased]` in `CHANGELOG.md` (Keep a Changelog: Added / Changed / Fixed / Removed), written for users, not as commit messages. The release workflow moves that section under the version, writes the released versions to `addon/CHANGELOG.md` (Home Assistant's add-on update dialog; generated by `.github/scripts/changelog.py addon`, never edited by hand, a test compares it) and uses the section as the release notes; it refuses to release with an empty `[Unreleased]`.
- **Commits:** short lowercase `<area>: <what>` subject (e.g. `gateway: retry lagging confirmations`), ending with the `Co-Authored-By` trailer.

## Safety-critical invariants (do not regress)

- A command is retried (locally with backoff, then once via the cloud) **only** after `ErrUnreachable` (or an unusable bridge client) or `ErrUnauthorized` — **never** after `ErrNoResponse` or any answer. A slow bridge that got `OPEN` must not get a second `OPEN`. Every attempt is signed afresh. Bridge/cloud HTTP clients use `DisableKeepAlives` so net/http never silently replays a request. Command pipeline: `internal/gateway/pipeline.go` (spec 5.8).
- The bridge answers every `/to_lock` with 200; only webhooks (`GO_TO_STATE_*` with the gateway key, then `STATE_CHANGED_*`) confirm a command. `/status` lags and is only a hint.
- Retained messages on `<base>/<id>/command`, `<base>/<id>/cloud_webhook` and `<base>/<id>/webhooks/set` are ignored (a retained `OPEN` must never unlatch the door on reconnect; a retained cloud webhook would replay an event, and a retained SetWebhooks request would be applied again, on every reconnect).
- SetWebhooks (spec 5.9) never writes to the bridge before the whole request is valid and its `revision` matches, never deletes or re-creates the gateway's own webhook, and makes one bridge call per step, never while a lock command is in progress.
- LOQED blocks an account after >12 cloud calls in 12 h. The gateway's `cloud_budget` (default 10, max 12) is persisted across restarts; crash loops must not exceed it.
- Bridge wire details: headers exactly `TIMESTAMP` / `HASH` (upper case on the wire), webhook timestamp tolerance ±20 s by default (`webhook.bridge_timestamp_tolerance`, 0 = off; the bridge delivers late when it has many webhooks), command query escaping replaces only `+` and `=`. State-reached events derive bolt state from `event_type`, never `requested_state`.
- HA must not show a definite state older than reality without `state_stale`.

## Secrets and real hardware

- `.env`, `.env.local`, `.env.*.local` hold real credentials (`LOQED_CLOUD_TOKEN`, …) and are git-ignored. `.env.example` is the committed template (placeholders only) — reading and editing it is fine.
- **Never read, print, grep, copy, source or edit the real secret files directly** — no Read/Edit tools, no shell commands on them (also enforced in `.claude/settings.json`). Checking existence (`test -f .env`) is fine. Programs may load `.env` themselves; their output must never echo secrets.
- A real lock and bridge (192.168.2.66) and a real cloud account exist. Checks against them are **read-only**: never send lock commands (bridge `/to_lock`, cloud `bolt_state`), and spend real cloud calls sparingly (12 per 12 h, account-wide). Use mocks/`httptest` for everything else.
- Scratch work (smoke tests, real-hardware checks) goes in `.superpowers/sdd/` (git-ignored), not the repo.

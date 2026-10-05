# go-loqed

Monorepo, module `github.com/t3hk0d3/go-loqed`, Go 1.27.

- **GoLoqed** — stateless client library: `bridge` (local Bridge API), `cloud` (cloud Lock API + cloud webhook parsing), `cloud/portal` (Integrations portal: login, mint/list/revoke tokens). Root package `loqed` holds shared types and error sentinels.
- **loqed-mqtt** — `cmd/loqed-mqtt`: local-first MQTT gateway with cloud fallback and Home Assistant discovery. Ships as a Docker image and an HA add-on (`addon/`).

The spec is the source of truth for behaviour and every config setting:
`docs/superpowers/specs/2026-10-04-loqed-mqtt-gateway-design.md`. Plans (with Global Constraints and Review Focus) live in `docs/superpowers/plans/`.

## Commands

```sh
go build ./...
go test -race ./...                         # full suite (CI runs exactly this)
go test ./internal/gateway -run TestName    # single test
gofmt -l .                                  # must print nothing
go vet ./...
go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.14.0 run ./...   # pinned; config in .golangci.yml
go test ./internal/hass -run TestDiscoveryPayload -update   # rewrite HA discovery golden file (review the diff)
python3 testdata/gen_vectors.py             # regenerate bridge signing golden vectors
docker build --target standalone .          # or --target addon
```

Before calling work done: `gofmt`, `go vet`, `go test -race ./...` and golangci-lint must all be clean.

## Layout and dependency rules

```
loqed.go errors.go flex.go   root package: shared types, error sentinels, lenient JSON numbers
bridge/                      local bridge client (HMAC-signed commands, webhooks, status)
cloud/  cloud/portal/        cloud Lock API, cloud webhooks, portal (Laravel/Inertia scraping)
internal/transport/          HTTP Do/Send with ErrUnreachable vs ErrNoResponse classification
cmd/loqed-mqtt/              main: wiring only (+ `healthcheck` subcommand)
internal/app/                top-level assembly and lifecycle
internal/config/             config load/validate (env > YAML > /data/options.json), Supervisor MQTT lookup
internal/auth/               cloud auth: token or email/password → minted token
internal/store/              credential cache /data/locks.json (atomic write, 0600)
internal/gateway/            per-lock supervisors, local/cloud/offline modes, failover, cloud budget
internal/webhook/            HTTP listener for bridge/cloud webhooks, /healthz
internal/hass/               MQTT client, topics, HA discovery, state/event publishing
internal/model/              normalized lock state/event model and mapping
internal/testutil/           in-process MQTT broker (mochi) for tests
addon/                       HA add-on config.yaml, DOCS.md, translations
```

- Library packages (`loqed`, `bridge`, `cloud`, `cloud/portal`, `internal/transport`) import **only the standard library**, and have **no goroutines, timers or package-level mutable state**. Every network method takes `context.Context` first.
- `internal/gateway` is the only package that knows local vs cloud. `internal/hass` is the only package that imports the MQTT library or knows topic names. They talk through a small interface plus a command channel.
- Bridges are addressed by IP only — never hostnames/mDNS.

## Conventions

- **Style:** `gofmt` + `.golangci.yml` (standard linters + errorlint, gosec, misspell, unconvert). Follow the surrounding code; keep comments sparse and explanatory.
- **Errors:** callers branch only on `loqed.ErrUnauthorized`, `ErrRateLimited`, `ErrUnreachable` (provably not delivered), `ErrNoResponse` (may have been delivered), `ErrBadSignature`, `ErrStaleTimestamp`, `ErrInvalidPayload`, or `*loqed.APIError`. Wrap with `%w`; use `errors.Is/As`.
- **No secrets in errors or logs:** never include tokens, passwords, keys, signed commands, URLs/query strings, headers, portal HTML, full cloud responses or cloud webhook bodies — this includes context-canceled and invalid-address errors. Cloud webhook decoding must never decode `key_name_admin`, `key_account_e-mail`, `key_account_name`.
- **Logging:** `log/slog`; mode transitions at info; repeated identical warnings are rate-limited.
- **Tests:** stdlib `testing` only, table tests against `httptest` servers, fixtures/golden files under `testdata/`. Test names read as behaviour (`TestCommandErrorDoesNotLeakSignedCommand`). Gateway tests use fake bridge/cloud interfaces and an injectable clock — no real sleeps. Integration tests use `internal/testutil` (in-process broker).
- **Commits:** short lowercase `<area>: <what>` subject (e.g. `gateway: retry lagging confirmations`), ending with the `Co-Authored-By` trailer.

## Safety-critical invariants (do not regress)

- A command is resent via the cloud **only** after `ErrUnreachable` (or an unusable bridge client) or `ErrUnauthorized` — **never** after `ErrNoResponse`. A slow bridge that got `OPEN` must not get a second `OPEN` via the cloud. Bridge/cloud HTTP clients use `DisableKeepAlives` so net/http never silently replays a request.
- Retained messages on `<base>/<id>/command` are ignored (a retained `OPEN` must never unlatch the door on reconnect).
- LOQED blocks an account after >12 cloud calls in 12 h. The gateway's `cloud_budget` (default 10, max 12) is persisted across restarts; crash loops must not exceed it.
- Bridge wire details: headers exactly `TIMESTAMP` / `HASH` (upper case on the wire), webhook timestamp tolerance ±10 s, command query escaping replaces only `+` and `=`. State-reached events derive bolt state from `event_type`, never `requested_state`.
- HA must not show a definite state older than reality without `state_stale`.

## Secrets and real hardware

- `.env`, `.env.local`, `.env.*.local` hold real credentials (`LOQED_CLOUD_TOKEN`, …) and are git-ignored. `.env.example` is the committed template (placeholders only) — reading and editing it is fine.
- **Never read, print, grep, copy, source or edit the real secret files directly** — no Read/Edit tools, no shell commands on them (also enforced in `.claude/settings.json`). Checking existence (`test -f .env`) is fine. Programs may load `.env` themselves; their output must never echo secrets.
- A real lock and bridge (192.168.2.66) and a real cloud account exist. Checks against them are **read-only**: never send lock commands (bridge `/to_lock`, cloud `bolt_state`), and spend real cloud calls sparingly (12 per 12 h, account-wide). Use mocks/`httptest` for everything else.
- Scratch work (smoke tests, real-hardware checks) goes in `.superpowers/sdd/` (git-ignored), not the repo.

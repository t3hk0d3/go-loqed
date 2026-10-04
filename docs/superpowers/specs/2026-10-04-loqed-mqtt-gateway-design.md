# go-loqed: LOQED library + loqed-mqtt gateway — Design

Date: 2026-10-04
Status: Draft, awaiting review

## 1. Purpose and context

Home Assistant's built-in LOQED integration is hard to change (core PR reviews are slow). This project moves LOQED support out of HA core into a standalone gateway that exposes locks over MQTT using HA MQTT Discovery, so lock behavior can be iterated on independently while HA only consumes standard entities.

Two parts in one repo (`github.com/t3hk0d3/go-loqed`, directory `~/dev/go-loqed`):

- **GoLoqed** — a thin, stateless Go client library for the LOQED local Bridge API and the LOQED cloud Integrations API.
- **loqed-mqtt** — a gateway service that owns all policy: credential caching, local-first operation with automatic cloud fallback, MQTT, and HA discovery. Shipped as a Docker image and as a Home Assistant add-on from the same repo.

### Success criteria

1. With only a cloud personal access token and MQTT settings, every lock on the account appears in HA as one device with lock, sensor, binary_sensor and event entities.
2. In normal operation the gateway talks only to the local bridge; state changes arrive via bridge webhooks and reach HA within ~1 s.
3. If the bridge is unreachable, lock/unlock/open still works via the cloud and HA shows `cloud` connection mode; the gateway returns to local automatically when the bridge recovers.
4. The gateway never exceeds LOQED's documented cloud request limits, and keeps bridge `/status` load minimal.
5. Restarts do not require the cloud when a valid credential cache exists.

### Non-goals (v1)

Cloud lock settings (open-house mode, twist assist, touch-to-connect, …); cloud-relayed webhooks (require a public URL); mDNS discovery or hostname resolution; MQTT 5; a custom HA component.

## 2. Background: LOQED APIs (verified 2026-10-04)

Sources: LOQED support docs (updated June 2026), `loqedAPI` 2.1.16 (pinned by HA core `dev`), HA core `loqed` integration, openHAB LOQED binding (merged 2026-09-21). Local 2023 checkouts in `~/dev` are outdated and were used only as secondary reference.

### 2.1 Local Bridge API (`http://<bridge_ip>`)

- `GET /status` — unauthenticated. Fields: `battery_percentage`, `battery_type`, `battery_type_numeric`, `battery_voltage`, `bolt_state` (`unknown|open|day_lock|night_lock`), `bolt_state_numeric`, `bridge_mac_wifi`, `bridge_mac_ble`, `lock_online` (0/1), `webhooks_number`, `ip_address`, `up_timestamp`, `wifi_strength`, `ble_strength`. LOQED recommends requesting status rarely (guidance: about once per day).
- `GET /to_lock?command_signed_base64=<urlquoted base64>` — signed command. Binary layout (all integers big-endian):
  ```
  message_id   u64 = 0
  protocol     u8  = 2
  command_type u8  = 7
  timestamp    u64 = unix seconds
  hmac         [32]byte = HMAC-SHA256(key = b64decode(key_secret),
                 msg = protocol | command_type | timestamp | key_id | device_id | action)
  key_id       u8  = local_id
  device_id    u8  = 1
  action       u8  = 1 open | 2 unlock (day_lock) | 3 lock (night_lock)
  ```
- Webhook management. Headers `TIMESTAMP` (decimal unix seconds) and `HASH` (hex SHA-256), where `K = b64decode(bridge_key)` and `ts8` = timestamp as u64 BE:
  - `GET /webhooks` — `HASH = sha256(ts8 | K)`. Returns list of `{id, url, trigger_*...}`.
  - `POST /webhooks` — body `{url, trigger_state_changed_open, trigger_state_changed_latch, trigger_state_changed_night_lock, trigger_state_changed_unknown, trigger_state_goto_open, trigger_state_goto_latch, trigger_state_goto_night_lock, trigger_battery, trigger_online_status}` (each 0/1; bit 0..8 of a flags bitmap in that order). `HASH = sha256(url | flags as u32 BE | ts8 | K)`.
  - `DELETE /webhooks/{id}` — `HASH = sha256(id as u64 BE | ts8 | K)`.
- Incoming webhooks (bridge → gateway): `POST` with headers `TIMESTAMP`, `HASH = sha256(body | ts8 | K)`. Receiver must reject `|now − ts| > 10 s`. Payload families:
  - State reached: `{mac_wifi, mac_ble, requested_state, event_type, key_local_id}`; `event_type` ∈ `STATE_CHANGED_OPEN|LATCH|NIGHT_LOCK|UNKNOWN`, `*_REMOTE` variants, `MOTOR_STALL`, `GO_TO_STATE_TOUCH_TO_LOCK`, …
  - Going to state: `{mac_wifi, mac_ble, go_to_state (OPEN|DAY_LOCK|NIGHT_LOCK), event_type (GO_TO_STATE_*), key_local_id}`; `key_local_id` 255 = unknown, may be `null`.
  - Battery: `{mac_wifi, mac_ble, battery_type, battery_percentage}`.
  - Online status: `{mac_wifi, mac_ble, wifi_strength, ble_strength}`; `ble_strength = -1` means lock offline.
  Numeric fields may arrive as JSON strings or numbers; parsers must accept both.

### 2.2 Cloud Integrations API (`https://integrations.production.loqed.com`)

- Auth: `Authorization: Bearer <personal access token>` (created at `https://integrations.loqed.com/personal-access-tokens`).
- `GET /api/locks/` → `{"data": [lock...]}`. Documented fields: `id`, `name`, `model_name`, `battery_percentage`, `battery_type`, `bolt_state`, `party_mode`, `guest_access_mode`, `twist_assist`, `touch_to_connect`, `lock_direction`, `mortise_lock_type`, `supported_lock_states`. **Undocumented but used by HA core today:** `online`, `bridge_ip`, `bridge_hostname`, `local_id`, `key_secret`, `bridge_key`, `backend_key`, `bridge_mac_wifi`.
- `GET /api/locks/{id}/bolt_state/{open|day_lock|night_lock}` — change state.
- Rate limit (documented): status at most once per day; more than 12 requests → blocked for 12 hours. Which endpoints this covers is ambiguous, so the gateway treats it as applying to `/api/locks/`.

### 2.3 Verification items (must check against real hardware/account before v1)

- V1: `/api/locks/` still returns the undocumented credential fields.
- V2: Which cloud endpoints the 12-per-12h limit applies to; whether command calls count.
- V3: Create-webhook hash layout (flags as u32 BE) still matches loqedAPI 2.1.16 and current bridge firmware.
- V4: Bridge HTTP port (assumed 80) for the TCP liveness probe.
- V5: Unit of `wifi_strength`/`ble_strength` (status values look like %, docs call event values dB). Sensors publish raw values; unit/device_class chosen after verification.

## 3. Architecture

```
go-loqed/                      module github.com/t3hk0d3/go-loqed
├── bridge/                    GoLoqed: local bridge client (stateless)
├── cloud/                     GoLoqed: cloud client (stateless)
├── cmd/loqed-mqtt/            main: wiring only
├── internal/
│   ├── config/                config loading/validation, Supervisor MQTT lookup
│   ├── store/                 credential cache (/data/locks.json)
│   ├── gateway/               per-lock supervisors, failover, cloud budget
│   ├── webhook/               HTTP listener, routing to supervisors, /healthz
│   └── hass/                  MQTT client, topics, discovery, state/event publishing
├── Dockerfile
├── docker-compose.yml
├── addon/                     HA add-on (config.yaml, DOCS.md, translations)
└── .github/workflows/         test, lint, multi-arch image build
```

The existing 2023 draft (`pkg/loqed_bridge_api`) is replaced; only its type/test ideas carry over. `go.mod` is bumped to the current stable Go release.

Dependency rules:
- `bridge` and `cloud` import only the standard library. No goroutines, timers or state.
- `internal/gateway` is the only package that knows about local vs cloud.
- `internal/hass` is the only package that imports the MQTT library or knows topic names.
- `gateway` ↔ `hass` communicate via a small interface (publish state, publish event, publish availability) and a command channel.

## 4. GoLoqed library

### 4.1 `bridge`

```go
type Credentials struct {
    BridgeKey  string // base64
    KeySecret  string // base64
    LocalKeyID uint8
}

func New(host string, creds Credentials, opts ...Option) (*Client, error) // validates base64
// Options: WithHTTPClient(*http.Client), WithClock(func() time.Time), WithScheme/WithPort (tests)

func (c *Client) Status(ctx) (*Status, error)
func (c *Client) Command(ctx, Action) error           // ActionOpen, ActionUnlock, ActionLock
func (c *Client) ListWebhooks(ctx) ([]Webhook, error)
func (c *Client) CreateWebhook(ctx, url string, t Triggers) error // AllTriggers provided
func (c *Client) DeleteWebhook(ctx, id int) error

// Pure verification + decoding of an incoming webhook.
func ParseEvent(bridgeKey []byte, body []byte, hash, timestamp string, now time.Time) (Event, error)
```

`Event` is an interface implemented by `StateReachedEvent`, `GoToStateEvent`, `BatteryEvent`, `OnlineEvent`. Each carries `MacWifi`, `MacBLE`; state events carry raw `EventType`, target/requested `BoltState`, and `KeyLocalID *uint8` (nil if null/absent).

Signing helpers (`signCommand`, `webhookHash`) are unexported but covered by golden-vector tests.

### 4.2 `cloud`

```go
func New(token string, opts ...Option) *Client   // WithBaseURL, WithHTTPClient
func (c *Client) ListLocks(ctx) ([]Lock, error)
func (c *Client) Command(ctx, lockID string, s BoltState) error // open|day_lock|night_lock
```

`Lock` includes all documented fields plus the undocumented local fields as optional (`*string`/`*uint8`), and a `HasLocalCredentials()` helper.

### 4.3 Errors (both packages)

Sentinels usable with `errors.Is`: `ErrUnauthorized` (401/403 auth failures), `ErrRateLimited` (429, or cloud block response), `ErrUnreachable` (dial errors, timeouts, connection reset), `ErrBadSignature`, `ErrStaleTimestamp`, `ErrInvalidPayload`. Other HTTP failures return `*APIError{StatusCode, Body}` (body truncated, secrets never included). Gateway policy branches only on these types.

## 5. Gateway (loqed-mqtt)

### 5.1 Configuration (`internal/config`)

Precedence: environment variables > YAML file (`--config`) > add-on `/data/options.json`. Every key is available in every source (env: `LOQED_` prefix, upper snake case, nesting with `__`, e.g. `LOQED_MQTT__URL`).

```yaml
cloud_token: ""              # required
locks: []                    # allow-list by lock name or id; empty = all locks on account
lock_settings:               # optional, keyed by lock id or name; all fields optional
  <id-or-name>:
    bridge_ip: ""            # pin IP regardless of cloud data
    bridge_key: ""           # manual local credentials (used when cloud lacks them)
    key_secret: ""
    local_id: 0
    key_names:               # key_local_id -> display name for events
      1: "Alice"
cache_path: /data/locks.json
cache_max_age: 0             # 0 = never expire by age
reconcile_interval: 24h      # max interval between /status reconciles in local mode
liveness_interval: 60s       # TCP probe interval in local mode
cloud_budget: 10             # max /api/locks/ calls per rolling 12h (account-wide)
webhook:
  listen: ":8099"
  url: ""                    # base URL override, e.g. http://10.0.0.5:8099; empty = auto
mqtt:
  url: ""                    # empty under Supervisor = auto from services API
  username: ""
  password: ""
  client_id: loqed-mqtt
  base_topic: loqed
homeassistant:
  enabled: true
  discovery_prefix: homeassistant
log_level: info
```

Validation fails fast with a clear message (missing token, bad durations, `lock_settings` entries matching no lock are warnings, not errors).

Supervisor: if `SUPERVISOR_TOKEN` is set and `mqtt.url` is empty, `GET http://supervisor/services/mqtt` (Bearer `SUPERVISOR_TOKEN`) provides host/port/username/password/ssl.

### 5.2 Credential cache (`internal/store`)

File: `cache_path`, JSON, written atomically (temp + fsync + rename), mode `0600`. Contents: `version`, `token_sha256`, `fetched_at`, and per lock `id`, `name`, `model_name`, `bridge_ip`, `bridge_hostname` (informational only), `bridge_mac_wifi`, `local_id`, `key_secret`, `bridge_key`, `backend_key`.

Cloud refresh (`ListLocks` + save) is triggered when:
1. Cache missing or unparseable.
2. `token_sha256` ≠ hash of configured token.
3. An allow-listed lock is not in the cache.
4. Bridge returns `ErrUnauthorized` (keys rotated).
5. Bridge unreachable at cached IP (see 5.4); if the refreshed IP differs, retry local with it.
6. `cache_max_age > 0` and exceeded.

Refreshes consume the shared cloud budget (5.5) and are additionally limited to one per 5 minutes per lock-trigger. If the cloud is unreachable but a cache exists, start from cache. If neither exists, exit non-zero with a clear message.

Connection target is always an IP (`lock_settings.bridge_ip` > cache). Hostnames/mDNS are never resolved.

Locks whose cloud data lacks local credentials and have no manual credentials in `lock_settings` run **cloud-only** (never enter `local`).

### 5.3 Startup

1. Load config; resolve MQTT settings (Supervisor if applicable).
2. Load cache; refresh from cloud per 5.2 rules 1–3, 6.
3. Apply allow-list; build per-lock `bridge.Client` (when credentials exist).
4. Start webhook listener, MQTT client, and one supervisor goroutine per lock.
5. Publish discovery (if enabled), availability, and initial state.

### 5.4 Per-lock supervisor and failover (`internal/gateway`)

One goroutine per lock owns all its state; inputs (webhook events, MQTT commands, timers) arrive on channels. A failure in one lock never affects others.

Modes: `local`, `cloud`, `offline`.

**Entering `local`:** `GET /status` (full snapshot), then ensure webhook registration:
- List webhooks; if our exact URL (`<base>/webhook/<lock-id>`) is absent, create it with all triggers.
- Delete webhooks whose URL path is `/webhook/<lock-id>` but host/port differ (stale gateway registrations). Never touch other webhooks.

**In `local`:**
- State comes from webhooks.
- Liveness: TCP connect to `bridge_ip:80` every `liveness_interval` (no HTTP request). Any received webhook also counts as liveness.
- `GET /status` only: on entering `local`; after a command if no matching webhook arrives within 10 s; when `bolt_state` is `unknown` (at most once per 10 min); otherwise at most once per `reconcile_interval`.
- 3 consecutive liveness/request failures (`ErrUnreachable`, 5 s timeout) → cache refresh rule 5. New IP → retry local. Same IP or refresh not possible → `cloud`.

**In `cloud`:**
- State from `cloud.ListLocks()` within budget (5.5).
- Bridge liveness probe continues every `liveness_interval`; on first success → enter `local`.
- 3 consecutive cloud failures (`ErrUnreachable`/5xx) → `offline`. `ErrRateLimited` does not go offline; it marks state stale and backs off (5.5).

**In `offline`:** entities unavailable. Retry bridge probe and (budget permitting) cloud every 5 minutes, indefinitely. First success → corresponding mode.

**Commands (LOCK/UNLOCK/OPEN):**
- Serialized per lock; a command older than 10 s when dequeued is dropped with a warning.
- `local`: signed bridge command.
  - `ErrUnreachable` → immediately retry via cloud (if lock has cloud access), count toward health.
  - `ErrUnauthorized` → cache refresh (rule 4), retry local once, then cloud.
- `cloud`: cloud command; publish transitional state (`locking`/`unlocking`/`opening`) immediately; one confirmation poll ~5 s later (budget permitting).
- `offline`: rejected, logged.

**Availability:** per-lock `online` iff mode ≠ `offline` and lock is online (`lock_online=1` from status / cloud `online`, not overridden by an online-status event with `ble_strength=-1`).

### 5.5 Cloud request budget

Account-wide token bucket shared by all locks and cache refreshes: at most `cloud_budget` (default 10) `GET /api/locks/` calls per rolling 12 h. One `ListLocks` call serves all locks. Spend priority: (1) cache refresh needed to reach `local`, (2) post-command confirmation, (3) background state polls, spaced evenly over the remaining window. When exhausted, state is published unchanged with `state_stale: true`. On `ErrRateLimited`, suspend all cloud reads for 12 h and log at error level. Cloud command calls are not budgeted (pending V2) but are logged with counts.

### 5.6 State and event mapping

Bolt state → HA lock state:

| Input | HA state |
|---|---|
| `night_lock` / `STATE_CHANGED_NIGHT_LOCK[_REMOTE]` | `LOCKED` |
| `day_lock`, `latch` / `STATE_CHANGED_LATCH[_REMOTE]` | `UNLOCKED` |
| `open` / `STATE_CHANGED_OPEN[_REMOTE]` | `OPEN` |
| `GO_TO_STATE_*` toward `NIGHT_LOCK` | `LOCKING` (only if current ≠ locked) |
| `GO_TO_STATE_*` toward `DAY_LOCK` | `UNLOCKING` (only if current ≠ unlocked) |
| `GO_TO_STATE_*` toward `OPEN` | `OPENING` (only if current ≠ open) |
| `MOTOR_STALL`, `unknown` | `JAMMED` |

Event entity normalized `event_types`: `locked`, `unlocked`, `opened`, `locking`, `unlocking`, `opening`, `jammed`, `unknown`. Unrecognized raw event types map to `unknown` (never dropped).

Event attributes: `reason` (raw `event_type`), `source` (parsed from raw type: `touch`, `remote`, `ble`, `twist_assist`, `instant_open`, `manual`, `other`), `key_local_id` (null if absent or 255), `key_name` (from `lock_settings.<lock>.key_names`, if configured).

Events are emitted only from bridge webhooks (local mode). No synthetic events from cloud polling. Event delivery is best-effort (webhooks can be lost); documentation states that automations about *whether* the door is locked must use the lock entity.

Battery and online-status webhook events update the state document but do not produce event-entity events.

## 6. MQTT and Home Assistant (`internal/hass`)

Library: `github.com/eclipse/paho.mqtt.golang` (MQTT 3.1.1), auto-reconnect, LWT. On reconnect: resubscribe, republish discovery (if enabled), availability and current state.

### 6.1 Topics (`<base>` = `mqtt.base_topic`, `<id>` = cloud lock id)

| Topic | Retained | Payload |
|---|---|---|
| `<base>/status` | yes | `online`/`offline` (LWT `offline`) |
| `<base>/<id>/availability` | yes | `online`/`offline` |
| `<base>/<id>/state` | yes | JSON state document |
| `<base>/<id>/event` | **no** | JSON `{event_type, reason, source, key_local_id, key_name}` |
| `<base>/<id>/command` | no (subscribe, QoS 1) | `LOCK`/`UNLOCK`/`OPEN` |

State document:
```json
{"lock":"LOCKED","bolt_state":"night_lock","battery_percentage":78,"battery_voltage":10.37,
 "wifi_strength":73,"ble_strength":20,"lock_online":true,"mode":"local",
 "last_event":"GO_TO_STATE_TOUCH_TO_LOCK","last_key_id":255,
 "last_event_at":"2026-10-04T12:00:00Z","state_stale":false}
```

These topics are published regardless of `homeassistant.enabled`, so the gateway is usable as a plain MQTT bridge.

### 6.2 Discovery

When `homeassistant.enabled`:
- One retained device-based discovery message per lock at `<discovery_prefix>/device/loqed_<id>/config`. All entities of a lock are components of that single device, so they are grouped per lock in HA. Device: name = lock name, manufacturer `LOQED`, model = `model_name`, identifiers = `loqed_<id>`, connections = bridge Wi-Fi MAC when known.
- Published at startup, on MQTT reconnect, and when `<discovery_prefix>/status` receives `online`.
- Locks no longer present (removed from account or allow-list) get an empty retained payload on their discovery topic, removing the device.
- `availability_mode: all` over `<base>/status` and `<base>/<id>/availability`.

Components per lock:

| Component | Platform | Details |
|---|---|---|
| Lock | `lock` | commands LOCK/UNLOCK/OPEN; states from `lock` field incl. LOCKING/UNLOCKING/OPENING/JAMMED |
| Battery | `sensor` | device_class `battery`, % |
| Battery voltage | `sensor` | diagnostic, device_class `voltage`, V |
| Wi-Fi signal | `sensor` | diagnostic, unit per V5 |
| BLE signal | `sensor` | diagnostic, unit per V5 |
| Lock online | `binary_sensor` | diagnostic, device_class `connectivity` |
| Connection mode | `sensor` | diagnostic, device_class `enum`, options `local`/`cloud`/`offline` |
| Last change reason | `sensor` | raw `event_type`; attributes `key_local_id`, `key_name`, `last_event_at` |
| Lock event | `event` | `event_types` from 5.6; reads `<base>/<id>/event` |

## 7. Webhook listener (`internal/webhook`)

- `POST /webhook/<lock-id>`: look up lock (unknown → 404), verify via `bridge.ParseEvent` (bad hash → 401, stale timestamp → 401 and log observed clock skew), forward event to supervisor, respond 200. Body size limit 64 KiB.
- `GET /healthz`: 200 with JSON per-lock `{mode, available, last_event_at}`; 503 if MQTT disconnected.
- Webhook base URL: `webhook.url` if set; otherwise per lock `http://<local-ip>:<port>`, where `<local-ip>` is the source address the OS selects for reaching that lock's bridge IP (UDP connect, no packets sent).
- Requires accurate host time (NTP); documented.

## 8. Error handling and logging

- `log/slog`, text or JSON, level from config. Never log tokens, keys, signed commands, or full cloud responses.
- MQTT disconnects do not change lock modes; state continues to be tracked and is republished on reconnect.
- Mode transitions logged at info; repeated identical failures rate-limited in logs.
- Fatal at startup only for: invalid config, or no cache and no cloud access. Everything at runtime degrades, never exits.

## 9. Packaging

- **Dockerfile:** multi-stage; `CGO_ENABLED=0` static binary on `gcr.io/distroless/static:nonroot`; `HEALTHCHECK` is not available in distroless without a shell, so the binary supports `loqed-mqtt healthcheck` (calls `/healthz`).
- **Images:** `linux/amd64`, `linux/arm64`, `linux/arm/v7`, pushed to `ghcr.io/t3hk0d3/loqed-mqtt` by GitHub Actions on tags.
- **docker-compose.yml:** `network_mode: host` (so auto-detected webhook URL is the real LAN IP), volume for `/data`, env-based config.
- **Add-on (`addon/`):** `config.yaml` with `image: ghcr.io/t3hk0d3/loqed-mqtt`, `host_network: true`, `services: [mqtt:need]`, `map`/persistent `/data`, options schema mirroring 5.1 (token as `password` type), `watchdog` on `/healthz`. `DOCS.md` covers token creation, NTP requirement, best-effort events, and the cloud rate limit.

## 10. Testing

- **bridge/cloud:** table tests against `httptest` servers using fixture payloads for status, webhook list, and every event family (including `MOTOR_STALL`, `*_REMOTE`, null `key_local_id`, string-typed numbers). Error mapping tests for 401/403/429/5xx/timeouts.
- **Golden vectors:** signed command bytes and all webhook hashes generated once from `loqedAPI` 2.1.16 with fixed clock and keys; Go output must match byte-for-byte. Generator script committed under `testdata/`.
- **gateway:** state-machine tests with fake bridge/cloud interfaces and an injectable clock: local→cloud→offline→local, IP-change refresh, auth-error refresh, command fallback to cloud, stale-command drop, missed-webhook `/status` fallback, unknown-state recovery limit, budget exhaustion, 429 backoff, cloud-only locks.
- **store:** atomic write, `0600`, token-hash mismatch, corrupt file handling.
- **hass:** golden JSON for discovery and state documents; mapping tables for state and event normalization; `homeassistant.enabled=false` publishes no discovery.
- **Integration:** run the gateway against an in-process MQTT broker (`mochi-mqtt/server`) and fake bridge/cloud servers: discovery published → command in → signed bridge call out → webhook in → state and event published.
- **CI:** `go test -race ./...`, `golangci-lint`, image build.
- **Manual:** verification items V1–V4 against a real lock before v1 release.

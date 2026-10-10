# go-loqed: LOQED library + loqed-mqtt gateway — Design

Date: 2026-10-04
Status: Approved; revised 2026-10-04 after adversarial plan review (rev 2); revised 2026-10-05 after real-hardware verification (rev 2.1); revised 2026-10-06: cloud webhooks over MQTT (rev 2.2); revised 2026-10-06 after the pre-merge review (rev 2.3); revised 2026-10-08: configurable bridge webhook timestamp tolerance; revised 2026-10-08: bridge webhooks over MQTT (rev 2.4)

**Rev 2.1 (2026-10-05)** — all changes come from tests against a real LOQED Touch, bridge and account (2.5):
- Command pipeline redesigned (5.8): latest command wins, retries only when a request provably never left, 30 s / 10 s deadlines, local-then-cloud, confirmation from webhooks, retained `command_status` topic.
- `WebhookConfirm` 10 s → 30 s; bridge `/status` demoted to a hint that never overrides newer webhook state (5.5).
- `key_local_id` 255, `""`, `null` or absent means "no key" (a manual action by hand or a system action such as the automatic latch after an open); events carry `key_local_id` null (4.1, 4.2, 5.7); `source` is only `gateway` or null (revised 2026-10-06).
- Cloud webhooks carry the numeric internal lock id, not the API id → one cloud webhook URL per lock (5.4, 7); duplicate deliveries are dropped; cloud events may arrive before bridge events (5.7).
- Token lifetime and lock-key lifecycle: tokens expire (~182 days); revoking or expiring a token does not revoke its lock key; deleting the key in the app does (5.2).
- Portal login sends `remember: false` (4.3).

**Rev 2.2 (2026-10-06)** — cloud webhooks can also arrive over MQTT:
- New per-lock topic `<base>/<id>/cloud_webhook` (6.1) carrying a cloud webhook body, enabled by `mqtt.cloud_webhooks` (5.1). It lets a relay that the user already runs, typically a Home Assistant automation with a Nabu Casa webhook trigger, feed cloud webhooks without a reverse proxy or a public URL (9).
- HTTP and MQTT deliveries go through one decode-and-route path; per-lock routing and the `cloud_webhook_id` binding are unchanged (7).
- `internal/hass` is split into `internal/mqtt` (MQTT client and the gateway's topics) and `internal/mqtt/hass` (Home Assistant discovery) (3).
- LOQED's web API article shows an alphanumeric `lock_id` and `key_account_e-mail`; real payloads differ (2.2). Test fixtures use the real shapes; one documented-shape fixture stays.

**Rev 2.3 (2026-10-06)** — fixes from the pre-merge review of the whole branch:
- An unwritable cache stops the gateway at startup, before any cloud or portal call; the cloud webhook secret is resolved before the first cloud call. A restart loop can no longer spend the cloud budget or mint tokens (5.3, 5.4).
- A local command attempt never runs past the local cutoff, so a bridge that hangs on connect leaves the cloud attempt its time (5.8).
- Bridge webhook delivery is confirmed, not assumed: until a signed bridge webhook arrives, `/status` is read every `liveness_interval`, and missing webhooks are detected and reported (5.5).
- The other feed's copy of an event is recognized for 5 min, so a late copy cannot roll the state back (5.7).
- A failover triggered during a command runs after the command resolves (5.5, 5.8).

**Rev 2.4 (2026-10-08)** — bridge webhooks over MQTT, for troubleshooting late events:
- Each lock's bridge webhook list (id, URL, triggers) is published, retained, on `<base>/<id>/webhooks` and shown by a diagnostic "Bridge webhooks" sensor (6.1, 6.2, 5.9). Every webhook target delays the others (2.1), so the list explains late events at a glance.
- A `SetWebhooks` request on `<base>/<id>/webhooks/set` replaces the bridge's list: new entries are added, listed ones are kept, unlisted ones are removed (5.9). It is off unless `mqtt.bridge_webhook_control` is set (5.1), has no Home Assistant entity, and must carry the `revision` of the list it was made from, so changes are deliberate and never based on an outdated view.
- The gateway's own webhook is always kept and cannot be added, changed or removed through the topic.
- `bridge.Webhook` decodes the trigger flags; `bridge.ParseTriggers` and `Triggers.Names` convert between trigger names and the bitmap (4.1).

## 1. Purpose and context

Home Assistant's built-in LOQED integration is hard to change (core PR reviews are slow). This project moves LOQED support out of HA core into a standalone gateway that exposes locks over MQTT using HA MQTT Discovery, so lock behavior can be iterated on independently while HA only consumes standard entities.

Two parts in one repo (`github.com/t3hk0d3/go-loqed`, directory `~/dev/go-loqed`):

- **GoLoqed** — a thin, stateless Go client library for the LOQED local Bridge API, the LOQED cloud Integrations (Lock) API, and the Integrations portal (Management) API used to mint access tokens.
- **loqed-mqtt** — a gateway service that owns all policy: credential caching, local-first operation with automatic cloud fallback, MQTT, and HA discovery. Shipped as a Docker image and as a Home Assistant add-on from the same repo.

### Success criteria

1. With only LOQED credentials (a personal access token, or portal email + password) and MQTT settings, every lock on the account appears in HA as one device with lock, sensor, binary_sensor and event entities.
2. In normal operation the gateway talks only to the local bridge; state changes arrive via bridge webhooks and reach HA within ~1 s.
3. If the bridge is unreachable, lock/unlock/open still works via the cloud and HA shows `cloud` connection mode; the gateway returns to local automatically when the bridge recovers.
4. The gateway never exceeds LOQED's documented cloud request limits, and keeps bridge `/status` load minimal.
5. Restarts do not require the cloud when a valid credential cache exists.

### Non-goals (v1)

Cloud lock settings (open-house mode, twist assist, touch-to-connect, …); automatic registration of cloud webhooks (LOQED offers no API for it; the user registers the URL in the portal); OAuth flows; mDNS discovery or hostname resolution; MQTT 5; a custom HA component.

## 2. Background: LOQED APIs (verified 2026-10-04)

Sources: LOQED support docs (updated June 2026), `loqedAPI` 2.1.16 (pinned by HA core `dev`), HA core `loqed` integration (full commit history through 2026-10-01), openHAB LOQED binding (merged 2026-09-21), and the Integrations portal's own front-end (route table and page bundles, inspected 2026-10-04). Local 2023 checkouts in `~/dev` are outdated and were used only as secondary reference.

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
  The bridge answers **every** `/to_lock` with `200 "Message resent to the lock"`, even for a wrong key; the lock verifies the signature. The response therefore proves only delivery to the bridge, never acceptance (2.5). The lock rejects commands whose timestamp is 60 s or more in the past (exact tolerance untested), so a command is re-signed for every attempt.
- Webhook management. Headers `TIMESTAMP` (decimal unix seconds) and `HASH` (hex SHA-256), where `K = b64decode(bridge_key)` and `ts8` = timestamp as u64 BE:
  - `GET /webhooks` — `HASH = sha256(ts8 | K)`. Returns list of `{id, url, trigger_*...}` with the same nine `trigger_*` fields as `POST`, as numbers 0/1 (V11).
  - There is no update call: changing a webhook's triggers means delete and create, and the new registration gets a new id. `POST` does not return the new id.
  - `POST /webhooks` — body `{url, trigger_state_changed_open, trigger_state_changed_latch, trigger_state_changed_night_lock, trigger_state_changed_unknown, trigger_state_goto_open, trigger_state_goto_latch, trigger_state_goto_night_lock, trigger_battery, trigger_online_status}` (each 0/1; bit 0..8 of a flags bitmap in that order). `HASH = sha256(url | flags as u32 BE | ts8 | K)`.
  - `DELETE /webhooks/{id}` — `HASH = sha256(id as u64 BE | ts8 | K)`.
- Incoming webhooks (bridge → gateway): `POST` with headers `TIMESTAMP`, `HASH = sha256(body | ts8 | K)`. Receiver must reject requests missing either header (HA core returns 400 since 2026-10-01) and `|now − ts|` above the tolerance (20 s by default, `webhook.bridge_timestamp_tolerance`; loqedAPI uses 10 s). The bridge calls its registered webhooks one after another, so with many of them a webhook arrives late and a 13 s delay was seen on a real bridge (2026-10-08); a stale TIMESTAMP is not only clock skew. The bridge accepts any URL, including public HTTPS (HA registers Nabu Casa cloudhook URLs on the bridge). Payload families:
  - State reached: `{mac_wifi, mac_ble, requested_state, event_type, key_local_id}`; `event_type` ∈ `STATE_CHANGED_OPEN|LATCH|NIGHT_LOCK|UNKNOWN`, `*_REMOTE` variants, `MOTOR_STALL`, …. The reached state comes from `event_type`; `requested_state` is only what was asked for (see 4.1).
  - Going to state: `{mac_wifi, mac_ble, go_to_state (OPEN|DAY_LOCK|NIGHT_LOCK), event_type (GO_TO_STATE_*, e.g. GO_TO_STATE_TOUCH_TO_LOCK), key_local_id}`; `go_to_state` may be absent (target then comes from the event-type suffix); `key_local_id` 255 = unknown, may be `null`.
  - Battery: `{mac_wifi, mac_ble, battery_type, battery_percentage}`.
  - Online status: `{mac_wifi, mac_ble, wifi_strength, ble_strength}`; `ble_strength = -1` means lock offline.
  Numeric fields may arrive as JSON strings or numbers; parsers must accept both.
- Observed behavior (2.5): a remote command produces `GO_TO_STATE_MANUAL_LOCK_REMOTE_{LATCH,NIGHT_LOCK}` (or `GO_TO_STATE_INSTANTOPEN_OPEN`) 0.4–6 s after `/to_lock`, then `STATE_CHANGED_*` 4–16 s after it. `REMOTE` means "a key acted remotely" (bridge, cloud or the app over BLE); only `key_local_id` identifies who. A command for the state the lock is already in produces `GO_TO_STATE_*` and no `STATE_CHANGED_*`. After `STATE_CHANGED_OPEN` the lock returns to `STATE_CHANGED_LATCH` (key 255) by itself within ~3 s. Actions without a key (turning the knob by hand, the automatic latch, `STATE_CHANGED_UNKNOWN` after `MOTOR_STALL`) carry `key_local_id` 255 on the bridge and `""` in the cloud; they cannot be told apart reliably.
- The bridge delivers webhooks to its targets sequentially and updates `/status` afterwards: with 2 reachable targets `STATE_CHANGED_*` arrived at +4 s and `/status` caught up within 1–3 s; with 5 targets (some unreachable) `STATE_CHANGED_*` took 10–16 s and `/status` stayed stale for minutes. Webhooks are outbound connections from the bridge, so segmented networks need a firewall rule bridge → gateway `webhook.listen` port.

### 2.2 Cloud Lock API (`https://integrations.production.loqed.com/api`)

Public, documented API. This is what the rest of this spec calls "cloud".

- Auth: `Authorization: Bearer <personal access token>` (created at `https://integrations.loqed.com/personal-access-tokens`).
- `GET /api/locks/` → `{"data": [lock...]}`. Documented fields: `id`, `name`, `model_name`, `battery_percentage`, `battery_type`, `bolt_state`, `party_mode`, `guest_access_mode`, `twist_assist`, `touch_to_connect`, `lock_direction`, `mortise_lock_type`, `supported_lock_states`. **Undocumented but used by HA core today:** `online`, `bridge_ip`, `bridge_hostname`, `local_id`, `key_secret`, `bridge_key`, `backend_key`, `bridge_mac_wifi`.
- `GET /api/locks/{id}/bolt_state/{open|day_lock|night_lock}` — change state. Success is `204 No Content`. The cloud actuates with **the token's own lock key**: every personal access token gets its own key slot on each lock (`local_id`, `key_secret` in `/api/locks/`), and cloud commands appear in webhooks with that `key_local_id`. If that key was deleted in the LOQED app, commands fail with `404 {"message":"No query results for model [App\\Models\\ApiKey]."}` while reads keep working.
- Tokens are RS256 JWTs with an `exp` claim ~182 days after issue. Revoking or expiring a token does **not** delete its lock key: the key keeps working locally until deleted in the LOQED app (verified: a key used 5 min after its token was revoked still moved the lock). Deleting the key in the app revokes it. Freed key slots are reused.
- Response headers `x-ratelimit-limit: 60` / `x-ratelimit-remaining` are a generic per-minute throttle, not the 12-per-12 h rule.
- Rate limit (documented): status at most once per day; more than 12 requests → blocked for 12 hours. Which endpoints this covers is ambiguous, so the gateway treats it as applying to `/api/locks/`.
- Outgoing cloud webhooks: registered manually by the user per lock in the API section of `app.loqed.com` (no API to list/create/delete). LOQED POSTs JSON with **no signature** (verified: only `Accept`, `Content-Type`, `User-Agent: Java/1.8.0_*` headers). Payloads as observed (2.5):
  - State reached: `{event_type, requested_state, lock_id, key_local_id, key_name_user, key_name_admin, key_account_email, key_account_name, value1, value2, value3}`.
  - Going to state: `{event_type, go_to_state, lock_id, key_local_id, key_name_user, …same personal fields…}`.
  - Signal: `{ble_strength, wifi_strength, lock_id}`; battery: `{battery_percentage, lock_id}`; online: `{online: 0|1, lock_id}`.
  - `lock_id` is the **numeric internal id** (e.g. `6148`), not the `/api/locks/` id; the API does not expose it. `key_local_id` is a string (`"1"`), `""` when no key was involved. `value1..value3` are IFTTT-style fields (`value2` = key name, `value3` = **account e-mail in plain text**); like the other personal fields they are never decoded or logged.
  - LOQED's web API article (support.loqed.com, article 6127911, checked 2026-10-06) shows a different shape: an alphanumeric `lock_id` like the `/api/locks/` id, `key_account_e-mail`, a numeric `key_local_id`, one combined signal/battery event, and no `value1..3`. Nothing received so far matches it. Parsing accepts both shapes; routing never relies on `lock_id` (7).
  - Each event may be delivered twice ~4 s apart, and cloud copies usually arrive 0.1–0.8 s **before** the bridge webhook for the same event.

### 2.3 Integrations portal / Management API (`https://integrations.production.loqed.com`)

Undocumented. Laravel + Inertia web app where users log in with email/password and manage personal access tokens. HA core does not use it (it asks the user to paste a token). Routes from the portal's embedded route table:

| Method | Path | Purpose |
|---|---|---|
| GET | `/login` | sets `laravel_session` (and possibly `XSRF-TOKEN`) cookies; HTML carries `<meta name="csrf-token">` |
| POST | `/login` | body `{email, password, remember: false}` (the portal answers `remember: true` with HTTP 500), header `X-CSRF-TOKEN` (meta value) and `X-XSRF-TOKEN` (cookie, if present); no captcha/2FA |
| GET | `/personal-access-tokens` | Inertia page; props `tokens[]` (id, name, …) and, once after creation, `accessToken` (plaintext) |
| POST | `/create-personal-access-tokens` | body `{name}`; redirects to the list page carrying `accessToken` |
| DELETE | `/personal-access-tokens/{id}` | revoke token |
| POST | `/logout` | end session |

Inertia JSON is obtained by sending `X-Inertia: true` and `X-Inertia-Version` (from the page's `data-page.version`). A version mismatch returns 409 with `X-Inertia-Location`; Inertia only version-checks GET, so on a mutation the 409 arrives on the redirected GET and the flash data (`accessToken`) is re-flashed onto that location. The client therefore never re-sends a mutation: it loads the `X-Inertia-Location` page as HTML and reads its props. Only plain GET visits are retried. 419 = CSRF rejected (reported as a portal-changed error, not as bad credentials). Any visit that ends on the `Auth/Login` page means the session is not authenticated (`ErrUnauthorized`). `/oauth/token` (Laravel Passport) also exists but needs a client id/secret we do not have; not used.

### 2.4 Verification items (must check against real hardware/account before v1)

- V1: `/api/locks/` still returns the undocumented credential fields.
- V2: Which cloud endpoints the 12-per-12h limit applies to; whether command calls count.
- V3: Create-webhook hash layout (flags as u32 BE) still matches loqedAPI 2.1.16 and current bridge firmware.
- V4: Bridge HTTP port (assumed 80) for the TCP liveness probe.
- V5: Unit of `wifi_strength`/`ble_strength` (status values look like %, docs call event values dB). Sensors publish raw values; unit/device_class chosen after verification.
- V6: Whether cloud webhooks carry any signature header in practice. If they do, verify it in addition to the path secret.
- V7: Portal login + token mint flow end to end: cookie/XSRF vs meta CSRF handling, whether `remember` is needed, exact `accessToken`/`tokens[]` prop shapes, behavior with accounts that have 2FA or SSO.
- V8: Home Assistant OS: the add-on (host network) resolves the `host` returned by `/services/mqtt` (`core-mosquitto`); the add-on options form renders `lock_settings`; `/data/options.json` validates with defaults only.

- V9 (new): whether deleted key slots stay deleted across token re-mints; the exact stale-timestamp tolerance of the lock.
- V10 (rev 2.2): the Home Assistant relay end to end: a webhook trigger reached through its Nabu Casa URL receives LOQED's JSON body as `trigger.json`; the documented action republishes it unchanged (valid JSON, numbers kept as numbers); the gateway applies the event.
- V11 (rev 2.4): `GET /webhooks` on current firmware returns the nine `trigger_*` fields per entry, as 0/1 numbers or numeric strings, and they match the flags the webhook was created with (read-only check against the real bridge); whether the bridge limits the number of webhooks.

These are tracked as the final task of the gateway plan; `v1.0.0` is not tagged until each has a recorded outcome.

### 2.5 Real-hardware results (2026-10-05)

| Item | Outcome |
|---|---|
| V1 | ✅ `/api/locks/` returns `bridge_ip`, `bridge_key`, `key_secret`, `local_id`, `backend_key`; lock id is a string (`QnZk…`); `bridge_mac_wifi` came back empty |
| V2 | ✅ the 12-per-12 h limit applies to status reads only; commands do not count (LOQED, confirmed by the account owner 2026-10-06) |
| V3 | ✅ create (all triggers), list and delete webhooks work |
| V4 | ✅ port 80; every incoming bridge webhook verified with `HASH`/`TIMESTAMP` |
| V5 | values 0–100 (Wi-Fi 20–39, BLE 88–100), `-1` = lock offline → published as `%` |
| V6 | ✅ cloud webhooks are unsigned; the path secret is the only authentication |
| V7 | ✅ after the `remember: false` fix: no 2FA, meta CSRF, create → list → revoke → logout and re-mint all work |
| V8 | open |
| V11 | ✅ 2026-10-08: `GET /webhooks` returns `id` (number), `url` and all nine `trigger_*` fields as numbers 0/1 for each entry (2 entries, both created with all triggers). Partial trigger sets and a webhook count limit are not verified; tests cover both number and numeric-string values |

Other findings are folded into 2.1–2.3. Not adopted for v1 but recorded: an app.loqed.com "API-Config" JSON key (`lock_id`, `lock_key_local_id`, `lock_key_key`, `backend_key`, `bridge_key`, `bridge_ip`) also works, with cloud status via `app.loqed.com/API/lock_status.php` and self-signed cloud commands via `app.loqed.com/API/lock_command.php`; it needs one key per lock, so the per-account Integrations API stays primary.

## 3. Architecture

```
go-loqed/                      module github.com/t3hk0d3/go-loqed (GoLoqed library, standard library only)
├── bridge/                    GoLoqed: local bridge client (stateless)
├── cloud/                     GoLoqed: cloud Lock API client + cloud webhook parsing (stateless)
│   └── portal/                GoLoqed: Management API client (login, mint/list/revoke tokens)
├── internal/transport/        GoLoqed: HTTP send with ErrUnreachable/ErrNoResponse classification
├── loqed-mqtt/                module github.com/t3hk0d3/go-loqed/loqed-mqtt (gateway; replace => ../)
│   ├── cmd/loqed-mqtt/        main: wiring only
│   └── internal/
│       ├── config/            config loading/validation, Supervisor MQTT lookup
│       ├── store/             credential cache (/data/locks.json), minted token, cloud webhook secret
│       ├── gateway/           per-lock supervisors, failover, cloud budget
│       ├── webhook/           HTTP listener, routing to supervisors, /healthz
│       └── mqtt/              MQTT client, topics, state/event/command_status publishing, command and cloud_webhook inputs
│           └── hass/          Home Assistant discovery documents and topics
├── Dockerfile                 builds loqed-mqtt/ (context: the repository root)
├── docker-compose.yml
├── addon/                     HA add-on (config.yaml, DOCS.md, translations)
└── .github/workflows/         test, lint (both modules), multi-arch image build
```

The library and the gateway are separate Go modules (since 2026-10-10), so library users do not inherit the gateway's dependencies or Go version. Both share one version tag (`vX.Y.Z`); `internal/...` paths below are relative to `loqed-mqtt/`.

The existing 2023 draft (`pkg/loqed_bridge_api`) is replaced; only its type/test ideas carry over. `go.mod` is bumped to the current stable Go release.

Dependency rules:
- `bridge`, `cloud` and `cloud/portal` import only the standard library (plus `golang.org/x/net/publicsuffix` for the portal cookie jar if needed). No goroutines, timers or persistent state; the portal client's session lives only inside one `Login`-scoped value.
- `internal/gateway` is the only package that knows about local vs cloud.
- `internal/mqtt` is the only package that imports the MQTT library. It owns the gateway's own topics (`<base>/…`) and knows nothing about Home Assistant.
- `internal/mqtt/hass` owns everything Home Assistant specific: discovery documents, the discovery topic and HA's birth topic. It imports `internal/mqtt` for `Topics`, `TopicID` and `LockInfo`, never the MQTT library. The MQTT client receives it through a small `mqtt.Discovery` interface; with `homeassistant.enabled: false` no implementation is passed and nothing HA specific is subscribed or published.
- `gateway` ↔ `mqtt` communicate via a small interface (publish state, publish event, publish availability) and input channels (commands, cloud webhooks); `internal/app` wires them.

## 4. GoLoqed library

### 4.1 `bridge`

```go
type Credentials struct {
    BridgeKey  string // base64
    KeySecret  string // base64
    LocalKeyID uint8
}

func New(host string, creds Credentials, opts ...Option) (*Client, error) // validates base64 and that host is an IP[:port]
// Options: WithHTTPClient(*http.Client), WithClock(func() time.Time), WithScheme/WithPort (tests)

func (c *Client) Status(ctx) (*Status, error)
func (c *Client) Command(ctx, Action) error           // ActionOpen, ActionUnlock, ActionLock
func (c *Client) ListWebhooks(ctx) ([]Webhook, error) // Webhook{ID, URL, Triggers}; absent trigger fields are 0
func (c *Client) CreateWebhook(ctx, url string, t Triggers) error // AllTriggers provided
func (c *Client) DeleteWebhook(ctx, id int) error

// Trigger names are the bridge's field names without "trigger_", in bit order:
// state_changed_open, state_changed_latch, state_changed_night_lock,
// state_changed_unknown, state_goto_open, state_goto_latch,
// state_goto_night_lock, battery, online_status. "all" means AllTriggers.
func ParseTriggers(names []string) (Triggers, error) // unknown name or empty list → error naming it; duplicates allowed
func (t Triggers) Names() []string                   // ["all"] for AllTriggers, else the set names in bit order; bits above 8 ignored

// Pure verification + decoding of an incoming webhook. ParseEvent accepts
// |skew| ≤ MaxClockSkew (20 s); ParseEventWithin takes the tolerance, and 0
// skips the timestamp check (HASH is still verified).
func ParseEvent(bridgeKey []byte, body []byte, hash, timestamp string, now time.Time) (Event, error)
func ParseEventWithin(bridgeKey []byte, body []byte, hash, timestamp string, now time.Time, maxSkew time.Duration) (Event, error)
```

`Event` is an interface implemented by `StateReachedEvent`, `GoToStateEvent`, `BatteryEvent`, `OnlineEvent`. Each carries `MacWifi`, `MacBLE`; state events carry raw `EventType` and `KeyLocalID *int` (a real key 0..254; nil for 255, `null`, `""`, absent or out of range = no key).

Classification is by `event_type` prefix: `GO_TO_STATE_*` → `GoToStateEvent` (target from `go_to_state`); everything else with an `event_type` → `StateReachedEvent`. The reached bolt state of a `StateReachedEvent` is derived from `event_type` exactly like loqedAPI (`STATE_CHANGED_OPEN[_REMOTE]` → open, `…_LATCH` → day_lock, `…_NIGHT_LOCK` → night_lock, anything else → unknown), never from `requested_state` (which is only what was asked for and is kept as a raw `RequestedState` field). `MOTOR_STALL` sets `Jammed: true` with bolt unknown. The same rules apply to cloud webhooks.

Verification order is HASH first, then timestamp skew (`now.Unix() − ts`, integer seconds, |skew| ≤ the tolerance in whole seconds), so unauthenticated requests never produce clock-skew diagnostics.

Signing helpers (`signCommand`, `webhookHash`) are unexported but covered by golden-vector tests.

### 4.2 `cloud`

```go
func New(token string, opts ...Option) *Client   // WithBaseURL, WithHTTPClient
func (c *Client) ListLocks(ctx) ([]Lock, error)
func (c *Client) Command(ctx, lockID string, s BoltState) error // open|day_lock|night_lock
```

`Lock` includes all documented fields plus the undocumented local fields as optional (`*string`/`*int`), and a `HasLocalCredentials()` helper (requires both keys and `local_id` in 0..255). `Command` treats a non-JSON 2xx response or a final URL on `/login` as `ErrUnauthorized`, so an injected redirect-following HTTP client can never turn an auth failure into silent success.

```go
// Decodes an outgoing cloud webhook body (unsigned; caller authenticates the request).
func ParseWebhook(body []byte) (WebhookEvent, error)
```

`WebhookEvent` carries `LockID` (the numeric internal id as a string), `EventType`, target/requested `BoltState`, `KeyLocalID *int` with the same rules as 4.1 (255, `""`, `null` or absent → nil), `KeyNameUser`, and signal/battery/online fields when present. `key_name_admin`, `key_account_email` (and the legacy spelling `key_account_e-mail`), `key_account_name` and `value1..value3` are deliberately **not** decoded, so they cannot leak downstream.

### 4.3 `cloud/portal`

```go
func New(opts ...Option) *Client                              // WithBaseURL, WithHTTPClient
func (c *Client) Login(ctx, email, password string) (*Session, error)
func (s *Session) CreateToken(ctx, name string) (Token, error) // Token{ID, Name, Value}
func (s *Session) ListTokens(ctx) ([]TokenInfo, error)
func (s *Session) RevokeToken(ctx, id string) error
func (s *Session) Logout(ctx) error
```

`Login` sends `remember: false`. Each `Session` owns a private cookie jar; nothing persists beyond it. Bad credentials or an expired session → `ErrUnauthorized`; unexpected HTML/props shape or CSRF rejection (419) → `ErrInvalidPayload` (signals the portal changed). Portal errors never carry response bodies (portal HTML embeds the CSRF token).

### 4.4 Errors (all packages)

Sentinels usable with `errors.Is`: `ErrUnauthorized` (401/403 auth failures), `ErrRateLimited` (429, or cloud block response), `ErrUnreachable` (the request was **provably not delivered**: DNS/dial/connect failure, including a connect timeout), `ErrNoResponse` (the request may have been delivered but no complete response arrived: timeout after the request was written, connection reset, truncated body), `ErrBadSignature`, `ErrStaleTimestamp`, `ErrInvalidPayload`. The two transport sentinels are distinguished with an `httptrace` `WroteRequest` hook. Other HTTP failures return `*APIError{StatusCode, Body}` (body truncated, secrets never included). Gateway policy branches only on these types.

Errors never contain request URLs, query strings, headers or bodies (a bridge command URL contains a replayable signed command); this includes context cancellation and invalid-address errors. Inner errors are wrapped with `%w` so `context.Canceled`/`context.DeadlineExceeded` remain matchable.

## 5. Gateway (loqed-mqtt)

### 5.1 Configuration (`internal/config`)

Precedence: environment variables > YAML file (`--config`) > add-on `/data/options.json`. Every key is available in every source (env: `LOQED_` prefix, upper snake case, nesting with `__`, e.g. `LOQED_MQTT__URL`).

```yaml
cloud_token: ""              # personal access token; or use email/password below
cloud_email: ""              # portal login, used to mint a token when none is configured/valid
cloud_password: ""
locks: []                    # allow-list by lock name or id; empty = all locks on account
lock_settings:               # optional, keyed by lock id or name; all fields optional
  <id-or-name>:
    bridge_ip: ""            # pin IP regardless of cloud data
    bridge_key: ""           # manual local credentials (used when cloud lacks them)
    key_secret: ""
    local_id: 0
    key_names: "1=Alice,3=Bob"  # key_local_id -> display name; also accepts a map {1: Alice} or a list ["1=Alice"]
cache_path: /data/locks.json
cache_max_age: 0             # 0 = never expire by age (checked at startup and hourly at runtime)
reconcile_interval: 24h      # max interval between /status reconciles in local mode
liveness_interval: 60s       # TCP probe interval in local mode
event_dedup_enabled: true    # false = publish every delivery (bridge and cloud copies, cloud re-deliveries)
event_dedup_window: 10s      # drop an event repeating the latest one (same event_type and key, any feed) within this; max 30s
cloud_budget: 10             # max /api/locks/ calls per rolling 12h (account-wide)
webhook:
  listen: ":8099"
  private_url: ""            # base URL the bridge calls (LAN), e.g. http://10.0.0.5:8099; empty = auto-detect per lock
  public_url: ""             # internet-reachable base URL for cloud webhooks (reverse proxy); empty = cloud webhooks off
  cloud_secret: ""           # path secret for the cloud endpoint; empty = generate once and store in cache
  bridge_timestamp_tolerance: 20s  # accepted |now − TIMESTAMP| of a bridge webhook (late delivery or clock skew); 0 = no check (HASH still verified; a captured webhook could be replayed); otherwise at least 1s
mqtt:
  url: ""                    # empty under Supervisor = auto from services API
  username: ""
  password: ""
  client_id: loqed-mqtt
  base_topic: loqed
  cloud_webhooks: false      # accept cloud webhook bodies on <base>/<id>/cloud_webhook (6.1); counts as "cloud webhooks configured"
  bridge_webhook_control: false  # accept SetWebhooks requests on <base>/<id>/webhooks/set (5.9); the list is published either way
homeassistant:
  enabled: true
  discovery_prefix: homeassistant
log_level: info
log_format: text             # text | json
```

Validation fails fast with a clear message (neither `cloud_token` nor `cloud_email` and no cached token, bad durations, `webhook.public_url` with a path component). `cloud_email` without `cloud_password` is allowed (a cached minted token is used; minting is impossible and says so). `lock_settings` entries and allow-list names matching no lock are logged as warnings after lock data is loaded, not errors.

`/data/options.json` is decoded as JSON (not YAML), then applied with the same unknown-key checks as YAML; JSON map keys are strings, so numeric map keys (`key_names`) are parsed from strings in every source.

Supervisor: if `SUPERVISOR_TOKEN` is set and `mqtt.url` is empty, `GET http://supervisor/services/mqtt` (Bearer `SUPERVISOR_TOKEN`) provides host/port/username/password/ssl. If the returned host does not resolve (host-network add-on, V8), `127.0.0.1` is used with the same port. The broker URL is only ever logged in redacted form (`url.URL.Redacted`).

### 5.2 Cloud authentication

Token source, in order:
1. `cloud_token` from config, if set.
2. A previously minted token from the cache.
3. Mint: `portal.Login(cloud_email, cloud_password)` → `CreateToken("loqed-mqtt <install-id>")` → store value, id and `sha256(lowercased cloud_email)` in the cache → revoke other tokens with the same name (create first, so a failed create never leaves the user without a token) → `Logout`. `<install-id>` is 8 random hex characters generated once and stored in the cache, so the name is stable across container recreation (a Docker hostname is the container id).

A cached minted token is used only if its stored e-mail hash matches the configured `cloud_email` (changing accounts re-mints). When the Lock API returns `ErrUnauthorized` for a minted token, mint again, at most once per hour; the last mint time is persisted in the cache so restarts cannot bypass the limit. When it does so for a configured `cloud_token`, log an error and keep running from the cache. Mint failures are logged with a clear hint to set `cloud_token` manually; they never crash a gateway that has a usable cache. The password is only needed for minting, so it can be removed from the config once a token is cached.

Token lifetime and lock keys:
- The token's `exp` claim is read (JWT payload, no signature check). From 14 days before expiry the gateway logs a warning daily and publishes `token_expires_at` in the state document. A configured `cloud_token` only warns; a minted token is re-minted within the last 14 days (still at most once per hour).
- Every mint creates a new key slot on each lock, and revoking the old token does **not** remove its key (2.2). The gateway therefore mints only when needed (no token, account change, `ErrUnauthorized`, expiry within 14 days, key deleted) and never on a schedule. Documentation tells users that revoking the token does not revoke lock access: delete the gateway's key in the LOQED app.
- Local keys keep working after the token expires or is revoked, so an expired token never stops local operation; only cloud fallback and refreshes need a valid token.
- A cloud command answered with the `No query results for model [App\Models\ApiKey]` 404 means the token's lock key was deleted in the app: the command fails with error `key_deleted`, the error is logged with that explanation, and a minted token is re-minted (rate limits as above); a configured token keeps the error until the user replaces it. The cached local credentials of that key are dead too (the next local command would be silently ignored by the lock), so the lock's record is refreshed.

### 5.3 Credential cache (`internal/store`)

File: `cache_path`, JSON, written atomically (temp + fsync + rename), mode `0600`. Contents: `version`, `install_id`, `token_sha256` (identity of the token the lock data was fetched with), minted token `{id, value, email_sha256, minted_at}` if any, `cloud_secret` (if generated), `fetched_at`, `published_ids` (lock ids with retained MQTT/discovery topics), `budget` (`calls` timestamps in the rolling window and `blocked_until`), and per lock `id`, `name`, `model_name`, `bridge_ip`, `bridge_hostname` (informational only), `bridge_mac_wifi`, `local_id`, `key_secret`, `bridge_key`, `backend_key`, and `cloud_webhook_id` (the numeric id first seen in a cloud webhook on that lock's URL). An unknown `version` is logged at warn and treated as missing. A cache write failure is reported as a storage error naming the path (never as "cloud unavailable"). At startup an unwritable cache is fatal (5.4 step 2): without it the budget, a minted token and the learned data would not survive a restart, so a restart loop could exceed the account limit and add a lock key per mint. Once running, the gateway keeps running from memory after a write failure.

Refreshes **merge** per lock: a lock present in the new list replaces its record, except that local-credential fields (`bridge_ip`, `local_id`, `key_secret`, `bridge_key`) absent from the new data keep their cached values. An empty lock list while the cache has locks is not applied (logged at warn, treated as a failed refresh); a non-empty list that lacks a cached lock removes it.

Cloud refresh (`ListLocks` + save) is triggered when:
1. Cache missing or unparseable.
2. `token_sha256` ≠ hash of the token in use (configured or newly minted).
3. An allow-listed lock is not in the cache and the cache is older than 12 h (prevents a typo in `locks` from spending a call on every restart).
4. Bridge returns `ErrUnauthorized` (keys rotated).
5. Bridge unreachable at cached IP (see 5.5); if the refreshed IP differs, retry local with it.
6. `cache_max_age > 0` and exceeded (at startup and hourly while running).

Refreshes consume the shared cloud budget (5.6). Per lock and trigger they back off exponentially: 5 min after the first, doubling up to 6 h while refreshes keep returning the same data for that lock; a refresh that changes the lock's IP or keys resets the backoff. Rule 4 is skipped for a lock whose `bridge_key` or `key_secret` is pinned in `lock_settings` (a refresh cannot fix a wrong manual key); rule 5 is skipped when `bridge_ip` is pinned. If the cloud is unreachable but a cache exists, start from cache. If neither exists, exit non-zero with a clear message.

Connection target is always an IP (`lock_settings.bridge_ip` > cache). Hostnames/mDNS are never resolved.

Locks whose cloud data lacks local credentials and have no manual credentials in `lock_settings` run **cloud-only** (never enter `local`).

### 5.4 Startup

1. Load config; resolve MQTT settings (Supervisor if applicable).
2. Load cache and check that `cache_path` can be written (the `install_id` write, or a probe write when it exists); if not, exit non-zero with a storage error naming the path, before any cloud or portal call (rev 2.3). If `webhook.public_url` is set, resolve the cloud webhook secret (configured, cached, or generated and saved). Then resolve cloud token (5.2); refresh lock data per 5.3 rules 1–3, 6.
3. Apply allow-list; build per-lock `bridge.Client` (when credentials exist).
4. Start webhook listener, MQTT client, and one supervisor goroutine per lock.
5. Publish discovery (if enabled), availability, and initial state. Remove retained topics (discovery, state, availability) for ids in `published_ids` that are no longer selected, then store the new `published_ids`. The same removal runs whenever a runtime refresh drops a lock.
6. If `webhook.public_url` is set, log each lock's cloud webhook URL (`<public_url>/cloud/<cloud_secret>/<lock-id>`) once at info level so the user can register it for that lock at app.loqed.com. If `mqtt.cloud_webhooks` is set, log each lock's `cloud_webhook` topic once at info level.
7. A shutdown signal during startup is a clean exit (status 0).

### 5.5 Per-lock supervisor and failover (`internal/gateway`)

"Cloud webhooks configured" (used below and in 5.7) means `webhook.public_url` is set or `mqtt.cloud_webhooks` is true. It is configuration only: it does not depend on webhooks actually arriving.

One goroutine per lock owns all its state; inputs (webhook events, MQTT commands, timers) arrive on channels. A failure in one lock never affects others.

Modes: `local`, `cloud`, `offline`.

**Entering `local`:** requires a bridge client; if one cannot be built from the current record, the lock does not enter `local` (and leaves it if a credential refresh produced unusable data). `GET /status` (full snapshot), then ensure webhook registration:
- List webhooks; if our exact URL (`<private_base>/webhook/<lock-id>`, see 7) is absent, create it with all triggers.
- Delete webhooks whose URL path is `/webhook/<lock-id>` but host/port differ (stale gateway registrations). Never touch other webhooks.
- If registration fails, `local` is still entered, but registration is retried every 10 min (and at every reconcile) until it succeeds; while unregistered, webhook delivery counts as unconfirmed (below): `/status` is read every `liveness_interval` and cloud webhook events (if configured) drive state.

**In `local`:**
- State comes from webhooks.
- Liveness: TCP connect to `bridge_ip:80` every `liveness_interval` (no HTTP request). Any received webhook also counts as liveness. TCP probe failures and HTTP failures are counted separately: a successful TCP probe resets only the probe counter, never the HTTP counter (a bridge that accepts TCP but hangs on HTTP must still fail over).
- **Webhook delivery is confirmed, not assumed.** A successful registration only proves that the gateway reaches the bridge, not that the bridge reaches the gateway (a firewall or VLAN rule may block bridge → gateway). Delivery is *unconfirmed* on entering `local` and after every (re-)registration, and *confirmed* when a correctly signed bridge webhook arrives (deduplicated copies count). It becomes unconfirmed again, with a warning that bridge webhooks are not reaching the gateway and naming the webhook address and port to allow, when (a) a `/status` read shows a bolt state that differs from the current one although no bridge webhook arrived in the last `reconcile_interval`, or (b) a gateway command sent via the bridge ends its confirmation window without any bridge webhook having arrived since it was sent. While unconfirmed, `/status` is read every `liveness_interval` (local only, no cloud budget) and its bolt state is applied under the hint rules below; the warning is rate-limited (8).
- `GET /status` only: on entering `local`; every `liveness_interval` while webhook delivery is unconfirmed; `WebhookConfirm` (30 s) after a command or after any `GO_TO_STATE_*` event if no `STATE_CHANGED_*` event reaching the target arrives, and once more 60 s later if still unresolved; after `MOTOR_STALL` (once, 30 s later); when `bolt_state` is `unknown` (at most once per 10 min); otherwise at most once per `reconcile_interval`. A failed `/status` marks state stale.
- `/status` is a **hint** (2.1): its `bolt_state` is applied only if no webhook changed the bolt state in the last 5 min, or if it reports the expected target. A `/status` read within 3 min of a command (sent, or failed with `no_response`/`rejected` after it may have been delivered) or `GO_TO_STATE_*` that still shows the previous state is inconclusive: the bolt state is left unchanged and `state_stale` is set until a webhook or a later read resolves it. Battery and signal fields from `/status` are always applied.
- The bridge's webhook list is checked at registration; if it holds more than 3 other webhooks a warning explains that each webhook target delays events and `/status` (2.1).
- Cloud webhook events (if configured) are an equal feed: they change state like bridge events; repeats are dropped (5.7).
- 3 consecutive liveness failures, or 3 consecutive HTTP failures (`ErrUnreachable`/`ErrNoResponse`, 5 s timeout) → cache refresh rule 5. New IP → retry local. Same IP or refresh not possible → `cloud`. When the third failure is a command's local attempt, the refresh and the mode change run after that command resolves; the command's own cloud attempt (5.8) comes first.

**In `cloud`:**
- State is stale from entering `cloud` until the first fresh cloud data (poll or webhook) arrives.
- With cloud webhooks configured: state and events come from cloud webhooks (push). Polling drops to one reconcile per `reconcile_interval`, scheduled from the last successful poll (events never postpone it).
- Without: state from `cloud.ListLocks()` within budget (5.6).
- Freshness: `state_stale` becomes true when no fresh data (successful poll, accepted webhook) arrived within the expected interval plus 10 min (expected interval = background poll spacing without push, `reconcile_interval` with push).
- Bridge liveness probe continues every `liveness_interval`; on first success → enter `local`.
- Cloud reachability is decided by an unbudgeted TCP connect to the cloud host (port 443) every `liveness_interval`, not by API calls. 3 consecutive probe failures, or 3 consecutive failed API calls (`ErrUnreachable`/`ErrNoResponse`/5xx) → `offline`. `ErrRateLimited` does not go offline; it marks state stale and backs off (5.6).
- Poll and status results are applied only if they were fetched after the last applied event; a poll result never overwrites newer webhook state. A cloud lock record with `online` absent keeps the previous online value.

**In `offline`:** entities unavailable. Every 5 minutes, indefinitely: TCP-probe the bridge (success → `local`) and the cloud host (success → `cloud`). Neither probe spends cloud budget. A correctly signed bridge webhook received in `cloud` or `offline` is applied to state even if entering `local` then fails.

**Commands:** see 5.8.

**Availability:** per-lock `online` iff mode ≠ `offline` and lock is online (`lock_online=1` from status / cloud `online`, not overridden by an online-status event with `ble_strength=-1`). Any later state, battery or signal event with `ble_strength ≠ -1` from the lock marks it online again (cloud `online: 1` is not always sent after recovery).

### 5.6 Cloud request budget

Account-wide rolling window shared by all locks and cache refreshes: at most `cloud_budget` (default 10, max 12) `GET /api/locks/` calls per rolling 12 h. One `ListLocks` call serves all locks; results are shared for 30 s, except for confirmation polls, which only accept data fetched after the command. Spend priority and reserves: (1) post-command confirmation may use the whole budget; (2) cache refresh leaves 1 call; (3) background state polls leave 2 and are spaced `12h / cloud_budget` apart. Budget is taken only when a request is actually sent (not while no token is available). Background polls are skipped while cloud webhooks are configured and at least one was received in the last `reconcile_interval`; then only one reconcile poll per `reconcile_interval` is made. When exhausted, state is published unchanged with `state_stale: true`. On `ErrRateLimited`, suspend all cloud reads for 12 h and log at error level.

The window (call timestamps) and the 12 h block are persisted in the cache after every change and restored at startup (timestamps in the future are clamped to now), so crash loops and restarts cannot exceed the account limit. Cloud command calls are outside the budget: LOQED's limit applies to status reads only (V2, 2.5). Each cloud command is logged at info.

Worst case per rolling 12 h with defaults: 10 reads; commands are unlimited by the budget. A flapping bridge triggers refreshes at +5 min, +10, +20, +40, +80, +160, +320 (7 in the first 12 h per lock), and refreshes stop at the 1-call reserve.

### 5.7 State and event mapping

Bolt state → HA lock state:

| Input | HA state |
|---|---|
| `night_lock` / `STATE_CHANGED_NIGHT_LOCK[_REMOTE]` | `LOCKED` |
| `day_lock`, `latch` / `STATE_CHANGED_LATCH[_REMOTE]` | `UNLOCKED` |
| `open` / `STATE_CHANGED_OPEN[_REMOTE]` | `OPEN` |
| `GO_TO_STATE_*` toward `NIGHT_LOCK` | `LOCKING` (only if current ≠ locked) |
| `GO_TO_STATE_*` toward `DAY_LOCK` | `UNLOCKING` (only if current ≠ unlocked) |
| `GO_TO_STATE_*` toward `OPEN` | `OPENING` (only if current ≠ open) |
| `MOTOR_STALL` | `JAMMED` |
| `unknown` / `STATE_CHANGED_UNKNOWN` | unknown (`lock: null`; matches HA core since 2026-10-01) |

Event entity normalized `event_types`: `locked`, `unlocked`, `opened`, `locking`, `unlocking`, `opening`, `jammed`, `unknown`, `command_failed`. Unrecognized raw event types map to `unknown` (never dropped). `command_failed` carries `reason` = the command (`LOCK`/`UNLOCK`/`OPEN`) and `source` = `gateway`, plus an `error` attribute with the error class (`expired`, `offline`, `unreachable`, `no_response`, `unauthorized`, `rate_limited`, `failed`).

Event attributes: `reason` (raw `event_type`), `source`, `key_local_id` (null for no key), `key_name`. `source` is `gateway` when the key is the gateway's own `local_id` and a gateway command is in flight or was sent in the last 60 s (and on `command_failed`), otherwise `null` (revised 2026-10-06). How the lock was operated (touch, twist assist, instant open, PIN, remote key) is in the raw `reason`; the gateway does not parse it into categories.

The gateway is a faithful bridge: every state change the lock reports is published, including the automatic return to day_lock after an open (an `unlocked` event with source `unknown`). Only repeat deliveries of the same event are dropped (below).

`key_name` priority: `lock_settings.<lock>.key_names[key_local_id]` → cloud webhook `key_name_user` → null. Account e-mail and account/admin names are never decoded, published or logged.

Event sources by mode:
- `local`: bridge and cloud webhooks, as equal feeds. Whichever copy of an event arrives first is applied and published (cloud copies usually arrive first).
- Deduplication (revised 2026-10-06, after a real-hardware run): within `event_dedup_window` (default 10 s, max 30 s; off with `event_dedup_enabled: false`) an event is dropped when it (a) repeats the latest published event (same `event_type` and key, any feed: a cloud re-delivery, the other feed's copy, or the same action repeated, such as jiggling the knob, which is noise), or (b) is the other feed's copy of a recent published event whose copy has not arrived yet (the feeds interleave: a PIN unlock delivered cloud GO_TO, cloud STATE_CHANGED, then both bridge copies 2 s late). A real change back (day, night, day) is never dropped. Rule (b) also applies after the window, for up to 5 min (the pairing window): a published event whose copy from the other feed has not arrived yet still absorbs that copy, so a copy delivered late (bridge delivery can lag by minutes) is dropped instead of re-applied. The pairing window only pairs copies across feeds; it never drops a repeat from the same feed, and with a single feed it has no effect. Each feed delivers in order, so once the other feed has delivered a later event, an older event still waiting for its copy stops pairing (that copy was lost); a real change back is therefore never absorbed. A dropped cloud copy may still add `key_name` to the state document (no second event).
- `cloud`: cloud webhooks, if configured.
- Never from polling: no synthetic events from `/status` or `ListLocks` results. Event delivery is best-effort (webhooks can be lost); documentation states that automations about *whether* the door is locked must use the lock entity.

Battery and online-status webhook events update the state document but do not produce event-entity events.

### 5.8 Command pipeline

Per lock, owned by the supervisor goroutine.

- **Input:** `LOCK`/`UNLOCK`/`OPEN`, or JSON `{"command": ..., "id": ...}` (the optional `id`, max 64 printable characters, is echoed in `command_status`). Retained messages are ignored and logged (a retained `OPEN` must never unlatch the door on reconnect).
- **Latest command wins:** one pending slot. A newer command replaces a pending command that has not been written to the bridge or cloud yet (the replaced one ends `superseded`). A command already written cannot be recalled; the newer one runs after it resolves. A command equal to the pending or in-flight one is coalesced (its `id`, if any, follows the existing command's status).
- **Deadline:** absolute, from MQTT arrival: 30 s for `LOCK`/`UNLOCK`, 10 s for `OPEN`. No attempt starts after it, and it is the context of every actuation call. Commands are never preceded by refreshes or polls.
- **Retry rule:** retry only when the request **provably never left** (`ErrUnreachable`: connection refused, no route, DNS failure, TCP connect timeout). Never retry after the request was written (`ErrNoResponse`), after any bridge answer, or after any cloud answer. Every attempt is signed with a fresh timestamp (the lock rejects stale ones, 2.1).
- **Order:** local attempts with backoff 0.5 s, 1 s, 2 s, then every 2 s until 10 s before the deadline (OPEN: until 3 s before; this point is the local cutoff). Each local attempt's timeout is the 5 s request timeout or the time left until the local cutoff, whichever is shorter, so a bridge that hangs on connect never uses the time reserved for the cloud. Then, if still not delivered and the lock has cloud access, **one** cloud command. `ErrUnauthorized` from the bridge (nothing happened) goes straight to the cloud attempt, then a cache refresh (5.3 rule 4). Locks in `cloud` mode skip local attempts; `offline` locks fail immediately.
- **Confirmation:** a `200` from the bridge or `204` from the cloud means only `sent`. `accepted` = a `GO_TO_STATE_*` toward the target with the gateway's key (`local_id`; cloud commands use the same token key); `confirmed` = `STATE_CHANGED_*` reaching the target, or `accepted` while the lock already is in the target state and no `STATE_CHANGED_*` follows within 5 s; a `/status` read or cloud poll showing the target confirms only if the lock was in another state when the command was sent (a lock ignoring the command, e.g. after its key was deleted, still reads as its old state — observed 2026-10-06); `failed` = `MOTOR_STALL` with the gateway's key (`stalled`) or nothing within `WebhookConfirm` (30 s, `no_confirmation`; `/status` hint rules of 5.5 apply). In `cloud` mode without cloud webhooks the confirmation poll of 5.6 confirms or fails it.
- **Status topic** `<base>/<id>/command_status` (retained): `{"command","id","status","via","attempts","error","received_at","updated_at"}`. `status` ∈ `pending`, `sending`, `sent`, `accepted`, `confirmed`, `failed`, `expired`, `superseded`; `via` ∈ `local`, `cloud`; `error` ∈ `unreachable`, `no_response`, `rejected`, `unauthorized`, `key_deleted`, `rate_limited`, `stalled`, `no_confirmation`, `offline` or `null`. The `lock` state shows `LOCKING`/`UNLOCKING`/`OPENING` from `sent` on. Every `failed`, `expired` or rejected command also publishes a `command_failed` event (5.7).

Behavior (contract; tests come one per effect):

```
CommandPipeline (internal/gateway)
    Submit(command, id, receivedAt)
        When nothing is pending or in flight
            - publishes command_status pending with the given id
            - starts delivery immediately
        When a different command is pending and not yet written
            - replaces the pending command
            - publishes superseded for the replaced command
        When an equal command is pending or in flight
            - sends nothing new
            - reports the existing command's status under the new id
        When a command is in flight
            - keeps the new command pending until the in-flight one resolves
            - never retries the in-flight command if it ends not-delivered
        When the MQTT message is retained
            - ignores it and logs it
            - publishes no status
        When the lock is offline
            - publishes failed with error offline
            - publishes a command_failed event
    deliverLocal
        On bridge 200
            - publishes sent with via local and the attempt count
            - starts the 30 s confirmation window
        On ErrUnreachable (refused, no route, connect timeout)
            - signs every retry with a fresh timestamp
            - retries after 0.5 s, 1 s, 2 s, then every 2 s
            - stops local retries 10 s before the deadline (OPEN: 3 s)
            - ends every local attempt by that cutoff, even when the bridge hangs on connect
            - then sends once via the cloud when the lock has cloud access
            - runs a failover triggered by these failures (refresh, mode change) only after the command resolves
            - publishes failed with error unreachable when there is no cloud access
        On ErrNoResponse or any non-2xx bridge answer other than unauthorized
            - never resends locally or via the cloud
            - publishes failed with error no_response or rejected
            - sets state_stale, keeping the lock state, unless the lock already is in the target state
            - treats a /status read within 3 min still showing the previous state as inconclusive (5.5)
            - still runs confirmation (webhooks, then the /status hint)
        On ErrUnauthorized
            - sends once via the cloud when possible
            - triggers a cache refresh after the command resolves
        When the deadline passes before any request was written
            - publishes expired
            - sends nothing afterwards
    deliverCloud
        On 204
            - publishes sent with via cloud
        On 404 "No query results for model [App\Models\ApiKey]"
            - publishes failed with error key_deleted
            - re-mints a minted token, or logs the configured token as unusable for commands
        On ErrNoResponse or 5xx
            - never resends
            - sets state_stale and applies the /status hint rules like a bridge ErrNoResponse
            - schedules the confirmation poll
        On ErrRateLimited
            - publishes failed with error rate_limited
    confirm
        On GO_TO_STATE toward the target with the gateway's key
            - publishes accepted
        On STATE_CHANGED reaching the target
            - publishes confirmed
        On accepted while the lock already is in the target state and no STATE_CHANGED within 5 s
            - publishes confirmed
        On MOTOR_STALL with the gateway's key
            - publishes failed with error stalled
            - sets the lock state to JAMMED
        On no STATE_CHANGED within 30 s
            - publishes failed with error no_confirmation
            - sets state_stale
        On a /status read within 3 min that still shows the previous state
            - leaves the bolt state unchanged
            - keeps state_stale until a webhook or later read resolves it
```

### 5.9 Bridge webhook list and SetWebhooks

Purpose: the user can see which targets the bridge calls (each one delays events, 2.1) and can change that set deliberately. Changing it is never one tap away: there is no Home Assistant entity for it, it is off by default, and a request must name the exact list it changes.

**The list.** In `local`, the supervisor reads `GET /webhooks`:
- at every webhook registration check (5.5; this read already exists);
- when a `/status` read reports a `webhooks_number` different from the count in the last published list (no extra requests in steady state);
- after every SetWebhooks request, whatever its outcome, and on a `conflict`.

It publishes the result, retained, on `<base>/<id>/webhooks` (6.1). Nothing is read in `cloud` or `offline`; the last published list stays, and `fetched_at` shows its age. A failed read publishes nothing and is retried at the next occasion above. Reads use the 5 s bridge timeout, and their `ErrUnreachable`/`ErrNoResponse` failures count toward the HTTP failure counter like any bridge request (5.5).

Each entry is `{"id", "url", "triggers", "gateway"}`: the full URL as the bridge returns it, `triggers` from `Triggers.Names()`, and `gateway: true` for the gateway's own registration of this lock (exact URL `<private_base>/webhook/<lock-id>`, 7). Entries are sorted by `id`.

`revision` identifies the list: the first 16 hex characters of `sha256` over the entries in `id` order (`id`, full URL, trigger bitmap, each length-prefixed). It changes whenever any webhook is added, removed or re-created.

**SetWebhooks.** Only when `mqtt.bridge_webhook_control` is true does the gateway subscribe to `<base>/+/webhooks/set`. A request (6.1) carries `revision`, `webhooks` (the complete desired list, the gateway's own may be left out) and an optional `request_id` (up to 64 printable characters, echoed back). Each entry of `webhooks` is one of:
- `{"id": N}` — keep webhook N as it is;
- `{"id": N, "triggers": [...]}` — keep webhook N; if its triggers differ, re-create it with these triggers (delete, then create; it gets a new id);
- `{"url": "...", "triggers": [...]}` — keep the webhook with exactly this URL and these triggers if it exists; if the URL exists with other triggers, re-create it; otherwise add it. `triggers` defaults to `["all"]`.

Webhooks on the bridge that no entry refers to are removed. The gateway's own webhook is always kept: listing it (by id, or by its URL with all triggers) is allowed and changes nothing, also when it is not registered at the moment (registration stays with 5.5).

Validation runs on the whole request before any bridge write; any failure ends it as `failed` / `invalid` with a `detail` naming the entry (never the URL), and nothing changes. The checks that need the current list (ids, URLs of kept webhooks, the gateway's own webhook) run after the `revision` check below, so they always apply to the list the client saw:
- payload over 16 KiB, not a JSON object, missing `revision`, or `webhooks` missing or longer than 20 entries;
- an entry with both or neither of `id` and `url`, or other keys;
- an `id` not in the current list, or listed twice; a URL listed twice, or matching a kept webhook's URL;
- a URL that is not absolute `http`/`https` with a host, or is longer than 1024 bytes;
- a URL whose path is `/webhook/<this lock's id>` that is not exactly the gateway's own URL, or the gateway's own webhook with triggers other than `all` (the gateway manages its registration, 5.5);
- an unknown trigger name or an empty `triggers` list.

Then, in this order:
1. The lock must be in `local` with a bridge client; otherwise `failed` / `offline`.
2. `revision` must equal the current list's revision; otherwise `failed` / `conflict`, and the list is read again and republished so the client can retry with the new revision; the result carries that revision only if the read worked. The comparison uses the last list read by the supervisor; a request never forces a read before the check.
3. Deletions (unlisted webhooks, then the delete half of each re-creation) in `id` order, then creations in request order, one bridge call at a time with the 5 s timeout. The first failing call stops the request: `partial` if some call succeeded, else `failed`, with `error` = that call's class (`unreachable`, `no_response`, `rejected`, `unauthorized`). A deleted webhook whose re-creation did not run is reported in `removed`.
4. The list is read again and published; the result carries the new `revision` (absent if that read failed).

A request with nothing to change ends `ok` without bridge writes. Requests are handled by the lock's supervisor goroutine, one at a time, after a command in progress resolves; a command never waits for more than the bridge call that is running. A request queued behind another fails with `conflict` if the first one changed the list. At most 4 requests wait; a further one fails at once with `conflict` ("too many queued requests"). A request that cannot be handed to its lock at all (its message queue is full) also gets a result: `failed` / `conflict` ("the lock is busy"). Retained messages and messages for an unknown `<id>` are ignored with a warning, as on `cloud_webhook` (6.1). A payload that is not a JSON object gets a `failed` / `invalid` result with `request_id` null.

The result is published, not retained, on `<base>/<id>/webhooks/result`: `{"request_id", "status": "ok"|"partial"|"failed", "error", "detail", "removed": [ids], "added": [new ids], "kept": [ids], "revision"}`. New ids are matched by URL in the list read after the request; an added URL that appears more than once there is reported once. Each removal and creation is logged at info with the webhook id and the URL's scheme and host only (8).

**Trust.** Anyone allowed to publish to `<base>/<id>/webhooks/set` can redirect the lock's events to any URL or remove other integrations' webhooks; anyone allowed to read `<base>/<id>/webhooks` sees every webhook URL, including Home Assistant webhook ids and Nabu Casa cloudhook URLs (which accept events from anyone who knows them). Both are within what broker access already allows (commands, `cloud_webhook`), so broker ACLs are the protection; URLs are published in full. The documentation says that the sensor's attributes, and so these URLs, are kept in Home Assistant's history.

## 6. MQTT and Home Assistant (`internal/mqtt`, `internal/mqtt/hass`)

Library: `github.com/eclipse/paho.mqtt.golang` (MQTT 3.1.1), auto-reconnect, LWT. On reconnect: resubscribe, republish discovery (if enabled), availability and current state. The reconnect republish always reads the latest cached document at publish time, so it can never overwrite a newer state with an older one. Messages on the command topic with the retain flag set are ignored.

### 6.1 Topics (`<base>` = `mqtt.base_topic`, `<id>` = cloud lock id)

| Topic | Retained | Payload |
|---|---|---|
| `<base>/status` | yes | `online`/`offline` (LWT `offline`) |
| `<base>/<id>/availability` | yes | `online`/`offline` |
| `<base>/<id>/state` | yes | JSON state document (`lock` is `null` when unknown) |
| `<base>/<id>/event` | **no** | JSON `{event_type, reason, source, key_local_id, key_name}` (+ `error` for `command_failed`) |
| `<base>/<id>/command` | no (subscribe, QoS 1) | `LOCK`/`UNLOCK`/`OPEN`, or JSON `{"command":"UNLOCK","id":"<client id>"}` |
| `<base>/<id>/command_status` | yes | JSON command status (5.8) |
| `<base>/<id>/cloud_webhook` | no (subscribe, QoS 1; only with `mqtt.cloud_webhooks`) | a cloud webhook body as LOQED sends it (2.2), forwarded unchanged |
| `<base>/<id>/webhooks` | yes | JSON bridge webhook list (5.9) |
| `<base>/<id>/webhooks/set` | no (subscribe, QoS 1; only with `mqtt.bridge_webhook_control`) | JSON SetWebhooks request (5.9) |
| `<base>/<id>/webhooks/result` | **no** | JSON SetWebhooks result (5.9) |

**`cloud_webhook` input:**
- Subscribed as `<base>/+/cloud_webhook` after every (re)connect, only when `mqtt.cloud_webhooks` is true.
- Retained messages are ignored with a warning (a retained event would replay stale state on every reconnect). Messages for an unknown `<id>` are ignored with a warning. Payloads over 64 KiB are dropped.
- The payload is decoded and routed exactly like `POST /cloud/<secret>/<lock-id>` (7): the topic's `<id>` selects the lock, and the body's `lock_id` is bound as `cloud_webhook_id` on first use. A mismatching `lock_id`, an undecodable payload or a full queue drops the message with a warning (QoS 1 is already acknowledged; there is no reply channel). The payload is never logged.
- Relays forward the body unchanged; no field filtering is expected. The body includes the account e-mail and names, which reach the broker; the gateway never decodes or logs them (4.2, 8).
- Trust: anyone allowed to publish to `<base>/<id>/cloud_webhook` can inject lock events, which is less than what publishing to `<base>/<id>/command` allows. Broker ACLs are the protection, as for commands.
- Duplicates from QoS 1 redelivery or a relay retry are dropped by the event dedup (5.7) like any repeat.

State document:
```json
{"lock":"LOCKED","bolt_state":"night_lock","battery_percentage":78,"battery_voltage":10.37,
 "wifi_strength":73,"ble_strength":20,"lock_online":true,"mode":"local",
 "last_event":"GO_TO_STATE_TOUCH_TO_LOCK","last_key_id":255,
 "last_event_at":"2026-10-04T12:00:00Z","state_stale":false,"token_expires_at":"2027-04-05T19:15:26Z"}
```

Bridge webhook list and SetWebhooks (5.9):
```json
{"revision":"9f2c41d0a1b2c3d4","fetched_at":"2026-10-08T08:00:00Z","count":3,"webhooks":[
  {"id":3,"url":"http://192.168.2.10:8123/api/webhook/abc","triggers":["all"],"gateway":false},
  {"id":5,"url":"https://hooks.nabu.casa/xyz","triggers":["battery","online_status"],"gateway":false},
  {"id":7,"url":"http://192.168.2.20:8099/webhook/QnZk","triggers":["all"],"gateway":true}]}
```
```json
{"revision":"9f2c41d0a1b2c3d4","request_id":"cleanup-1",
 "webhooks":[{"id":3},{"url":"http://192.168.2.11:8123/api/webhook/def","triggers":["all"]}]}
```
```json
{"request_id":"cleanup-1","status":"ok","error":null,"detail":null,
 "removed":[5],"added":[8],"kept":[3,7],"revision":"0c1d2e3f4a5b6c7d"}
```

These topics are published regardless of `homeassistant.enabled`, so the gateway is usable as a plain MQTT bridge.

### 6.2 Discovery

When `homeassistant.enabled`:
- One retained device-based discovery message per lock at `<discovery_prefix>/device/loqed_<id>/config`. All entities of a lock are components of that single device, so they are grouped per lock in HA. Device: name = lock name, manufacturer `LOQED`, model = `model_name`, identifiers = `loqed_<id>`, connections = bridge Wi-Fi MAC when known.
- Published at startup, on MQTT reconnect, and when `<discovery_prefix>/status` receives `online`.
- Locks no longer present (removed from account or allow-list, at startup or after a runtime refresh) get an empty retained payload on their discovery, state, availability and `webhooks` topics, removing the device. Tracking uses `published_ids` in the cache.
- `availability_mode: all` over `<base>/status` and `<base>/<id>/availability`.

Components per lock:

| Component | Platform | Details |
|---|---|---|
| Lock | `lock` | commands LOCK/UNLOCK/OPEN; states from `lock` field incl. LOCKING/UNLOCKING/OPENING/JAMMED; `null` → unknown |
| Battery | `sensor` | device_class `battery`, % |
| Battery voltage | `sensor` | diagnostic, device_class `voltage`, V |
| Wi-Fi signal | `sensor` | diagnostic, `%` (V5) |
| BLE signal | `sensor` | diagnostic, `%` (V5); `-1` → unavailable |
| Lock online | `binary_sensor` | diagnostic, device_class `connectivity` |
| Connection mode | `sensor` | diagnostic, device_class `enum`, options `local`/`cloud`/`offline` |
| Last change reason | `sensor` | raw `event_type`; attributes `key_local_id`, `key_name`, `last_event_at` |
| Lock event | `event` | `event_types` from 5.7; reads `<base>/<id>/event` |
| Last command | `sensor` | diagnostic, `enum` of command statuses (5.8) from `command_status`; attributes `command`, `id`, `via`, `attempts`, `error`, `updated_at` |
| Token expires | `sensor` | diagnostic, device_class `timestamp`, from `token_expires_at` (absent when unknown) |
| Bridge webhooks | `sensor` | diagnostic, value `count` from `<base>/<id>/webhooks`; attributes `webhooks`, `revision`, `fetched_at` (an attribute payload must be a JSON object, so the array is nested); unknown until a list is published. No entity writes webhooks. |

## 7. Webhook listener (`internal/webhook`)

One HTTP listener (`webhook.listen`) serves:

- `POST /webhook/<lock-id>` (bridge, private): look up lock (unknown → 404); missing `TIMESTAMP`/`HASH` → 400; verify via `bridge.ParseEvent` (bad hash → 401, stale timestamp → 401 and log observed clock skew; only reachable with a valid hash); forward to supervisor (queue full → 503); 200.
- `POST /cloud/<cloud_secret>/<lock-id>` (cloud, public): only routed when `public_url` is set; wrong secret → 404 (constant-time compare); unknown `<lock-id>` → 404; decode via `cloud.ParseWebhook` (the same decode-and-route path as the MQTT `cloud_webhook` topic, 6.1); route by the **path** lock id (the body's numeric `lock_id` is stored as `cloud_webhook_id` on first use and later bodies with a different numeric id are rejected with 409 and a warning); forward to supervisor; 200. The path secret is the only authentication (see V6); it is 32 random bytes, base64url, and never logged except in the one startup line.
- `GET /healthz`: 200 with JSON `{mqtt_connected, locks: {<id>: {mode, available, last_event_at}}}`; 503 only if MQTT has been disconnected for more than 5 minutes (so a broker restart does not make the add-on watchdog restart the gateway).
- Body size limit 64 KiB on both webhook routes. Server timeouts: read header 5 s, read 15 s, write 15 s, idle 60 s. If the listener fails at runtime, `Run` returns the error (the process exits non-zero and is restarted) instead of silently running without webhooks.

URLs:
- Private (bridge) base: `webhook.private_url` if set; otherwise per lock `http://<local-ip>:<port>`, where `<local-ip>` is the source address the OS selects for reaching that lock's bridge IP (UDP connect, no packets sent).
- Public (cloud) base: `webhook.public_url` (scheme + host[:port] only; a path is a config error). Documentation tells users to expose only the `/cloud/` path through their reverse proxy, forwarded unchanged, and not to log request paths (the path is the secret).
- When the auto-detected private address is in a Docker bridge network (`172.16.0.0/12`) and the bridge IP is not, a warning tells the user to use host networking or set `webhook.private_url`.

Requires accurate host time (NTP) for bridge webhooks; documented.

## 8. Error handling and logging

- `log/slog`, text or JSON (`log_format`), level from config. Never log tokens, passwords, keys, the cloud webhook secret (except the single startup line), signed commands, URLs with credentials, full cloud responses, or cloud webhook bodies (they contain personal data).
- MQTT disconnects do not change lock modes; state continues to be tracked and is republished on reconnect.
- Mode transitions logged at info. Repeated identical warnings (same message and lock) are rate-limited: the first is logged, repeats within 10 min are counted and summarized in the next emitted line (`repeated=N`).
- Fatal at startup only for: invalid config, an unwritable cache (5.4 step 2), or no cache and no cloud access. Everything at runtime degrades, never exits.

## 9. Packaging

- **Dockerfile:** multi-stage; `CGO_ENABLED=0` static binary. Two final targets: `standalone` on `gcr.io/distroless/static-debian12:nonroot` with a `/data` directory owned by uid 65532 (so named volumes are writable), and `addon` on `gcr.io/distroless/static-debian12` (root; the Supervisor's `/data` is root-owned). Exec-form `HEALTHCHECK` runs `loqed-mqtt healthcheck`, which calls `/healthz` on the configured listen address (an unspecified host maps to 127.0.0.1). `LABEL org.opencontainers.image.source` links the package to the repo.
- **Images:** standalone `ghcr.io/t3hk0d3/loqed-mqtt` for `linux/amd64`, `linux/arm64`, `linux/arm/v7`; add-on `ghcr.io/t3hk0d3/loqed-mqtt-addon` for `linux/amd64`, `linux/arm64`. Pushed by the manually run `release` workflow after tests pass; `latest` only for non-prerelease versions. Packages must be made public once after the first push (documented release step).
- **docker-compose.yml:** `network_mode: host` (so auto-detected webhook URL is the real LAN IP), named volume for `/data`, env-based config.
- **Add-on (`addon/`):** `config.yaml` with `image: ghcr.io/t3hk0d3/loqed-mqtt-addon` (prebuilt; no `build.yaml`, no local build: current Supervisor no longer passes `BUILD_FROM`), `arch: [amd64, aarch64]`, `host_network: true` (so no `ports`), `services: [mqtt:need]`, persistent `/data`, options schema mirroring 5.1 with nesting depth ≤ 2 (`lock_settings` is a list of objects whose `key_names` is a `"1=Alice,3=Bob"` string; every nested key has a default; `webhook.listen` is not exposed), `cloud_token` and `cloud_password` as `password?`, no `watchdog` key (obsolete: the Supervisor watchdog uses the image `HEALTHCHECK`). Release order (automated by the `release` workflow): the add-on version bump commit is tagged and its images are built first; `master` moves to it last, so the Supervisor never sees a version without an image. Prereleases are tagged without a bump. `DOCS.md` covers token vs email/password setup, the `lock_settings`/`key_names` format, NTP requirement, best-effort events, the cloud rate limit, `private_url` when not on host networking, and optional cloud webhooks (reverse proxy exposing only `/cloud/`, registering each lock's logged URL for that lock at app.loqed.com; or, without a reverse proxy, a Home Assistant automation with one webhook trigger per lock, not local-only, whose Nabu Casa URL is registered for that lock and which publishes the body unchanged and non-retained to that lock's `cloud_webhook` topic), the firewall rule needed on segmented networks (bridge → gateway webhook port) and the warning logged when bridge webhooks do not arrive (the gateway then reads `/status` every minute), removing stale webhooks from the bridge (each target delays events) with the "Bridge webhooks" sensor and a SetWebhooks example (`mqtt.bridge_webhook_control`, revision, keep by id, what is removed, that results are only seen by a subscribed client, and that URLs are kept in HA history), deleting the gateway's key in the LOQED app to revoke its access (revoking the token is not enough), the `command_status` topic and JSON command payload, and recalibrating the lock if UNLOCK opens the door.

## 10. Testing

- **bridge/cloud:** table tests against `httptest` servers using fixture payloads for status, webhook list, and every bridge and cloud event family (including `MOTOR_STALL`, `*_REMOTE`, null `key_local_id`, string-typed numbers). Cloud webhook parsing must not expose e-mail/account fields. Cloud webhook fixtures use the shapes actually received (numeric `lock_id`, string `key_local_id`, separate signal/battery events, `key_account_email`, `value1..3`), plus one fixture in the documented shape. Error mapping tests for 401/403/429/5xx/timeouts.
- **cloud/portal:** fake Laravel/Inertia server: XSRF cookie handling, login success/failure, Inertia version 409 retry, token create/list/revoke, changed-props → `ErrInvalidPayload`.
- **Golden vectors:** signed command bytes and all webhook hashes generated once from `loqedAPI` 2.1.16 with fixed clock and keys; Go output must match byte-for-byte. Generator script committed under `testdata/`.
- **gateway:** state-machine tests with fake bridge/cloud interfaces and an injectable clock: local→cloud→offline→local, IP-change refresh, auth-error refresh, command fallback to cloud **only** on `ErrUnreachable` (never on `ErrNoResponse`), command deadline, stale-command drop, missed-webhook `/status` fallback, lost `STATE_CHANGED` after `GO_TO_STATE`, TCP-up/HTTP-hung bridge fails over, nil bridge client never used, unknown-state recovery limit, budget exhaustion, budget persistence across restart, refresh backoff under a flapping bridge, confirmation poll ignores pre-command cached data, 429 backoff, cloud-only locks, cloud probe-based offline detection and 5 min recovery, token minting and re-mint on 401, cloud-webhook push in `cloud` mode, cloud→bridge event enrichment and 30 s matching window in `local` mode, a late cross-feed copy inside the 5 min pairing window, unconfirmed webhook delivery (1-minute `/status`, warning on a missed change or a command confirmed without bridge webhooks, back to `reconcile_interval` after a bridge webhook), a bridge that hangs on connect during `OPEN` (cloud attempt still runs before the deadline), a failover triggered by a command running after that command. At least one test runs the supervisor against the real `CloudHub` + `Budget` (only the HTTP API faked).
- **store:** atomic write, `0600`, token-hash mismatch, corrupt file handling, minted token and generated secret persistence.
- **app:** an unwritable cache exits non-zero at startup without any cloud or portal request.
- **bridge webhooks (rev 2.4):** `ParseTriggers`/`Names` round trip for every name and `all`; `ListWebhooks` decodes trigger flags (numbers and numeric strings, absent fields). Supervisor tests with a fake bridge: list published on registration and on a `webhooks_number` mismatch; each validation failure leaves the bridge untouched; `conflict` re-reads; `offline` outside `local`; keep by id, keep by URL, re-create on changed triggers, add, remove; the gateway's own webhook is never deleted or re-created; a failing call mid-request gives `partial` with what was done and a republished list; a request waits for a command in progress. MQTT tests: no `set` subscription without `mqtt.bridge_webhook_control`, retained `set` ignored. One app-level test through the fake bridge and broker: set → bridge changed → list republished → result.
- **mqtt / hass:** golden JSON for discovery (in `hass`) and state documents; mapping tables for state and event normalization; `homeassistant.enabled=false` publishes no discovery; retained command messages are ignored; `cloud_webhook` messages reach the gateway only when enabled and not retained.
- **Integration:** run the gateway against an in-process MQTT broker (`mochi-mqtt/server`) and fake bridge/cloud servers: discovery published → command in → signed bridge call out → webhook in → state and event published.
- **CI:** `go test -race ./...`, `golangci-lint` (pinned version, run locally in the final task too), `gofmt` check, image build, add-on config lint.
- **Mock bridge realism:** the fake bridge and smoke-test mock accept every `/to_lock` with `200 "Message resent to the lock"` and act only on valid signatures with fresh timestamps; emit `GO_TO_STATE_*` after ~3 s and `STATE_CHANGED_*` after 10–16 s (configurable); lag `/status` behind webhooks, with a mode that keeps it stale; emit key 255 for actions without a key, including the automatic latch after open (the fake cloud sends `""`); the fake cloud returns 204 for commands, the `ApiKey` 404 for deleted keys, and sends cloud webhook copies before bridge copies, sometimes twice.
- **Command pipeline:** one test per effect in the 5.8 skeleton.
- **Manual:** verification items V8, V9, V10 against a real lock and account before v1 release (V1, V3–V7 recorded in 2.5).

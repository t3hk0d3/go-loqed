# loqed-mqtt rev 2.1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bring the library and the `loqed-mqtt` gateway on `feat/v1` in line with spec rev 2.1, which reflects what the real-hardware tests on 2026-10-05 showed.

**Architecture:** Most of the change is in `internal/gateway`. The synchronous `onCommand` is replaced by a per-lock command pipeline. The pipeline is owned by the supervisor goroutine and driven by its tick and by a wake timer. Confirmation now comes from webhooks, and `/status` is only a hint. The library learns the real "no key" markers and the deleted-key 404. `internal/auth` reads token expiry. `internal/hass` gains a retained `command_status` topic, JSON commands and two sensors. Cloud webhooks move to one URL per lock.

**Tech Stack:** Go 1.27; the existing dependencies; no new modules.

**Spec:** `docs/superpowers/specs/2026-10-04-loqed-mqtt-gateway-design.md` (rev 2.1; sections 2.1, 2.2, 4.1, 4.2, 5.2–5.5, 5.7, 5.8, 6, 7, 9, 10)

**Prerequisite:** Tasks 1–15 of `2026-10-04-loqed-mqtt-gateway.md` are implemented on `feat/v1`. Task 16 of that plan (the real-hardware gate) runs **after** this plan.

## Revisions after the real-hardware run (2026-10-06)

These supersede the matching parts of the tasks below (spec 5.7 is authoritative):
- No filtering of real state changes: the automatic latch after an open is published like any other event.
- Cloud and bridge are equal feeds; the first copy of an event is published. Dedup (configurable `event_dedup_enabled`, `event_dedup_window` 10 s, max 30 s) drops a repeat of the latest published event and the other feed's copy of a recent event (feeds interleave).
- `source` is `gateway` or null; no parsed categories (`touch`, `remote`, `unknown`, …).
- `GO_TO_STATE_*_VIA_OUTSIDE_MODULE_PIN` targets open.
- V2 answered: the cloud budget covers status reads only; cloud commands are no longer recorded in it (`Budget.Record` removed). This supersedes "cloud commands are never refused but are recorded" in the gateway plan's Global Constraints.

## Styleguide

The project styleguide is `CLAUDE.md` (Conventions, Safety invariants, Secrets) together with `.golangci.yml` (golangci-lint v2.14.0 with errorlint, gosec, misspell, unconvert; gofmt). Every task follows it. The rules that matter most here:

- Errors are wrapped with `%w` around the library sentinels (`loqed.Err*`). New sentinels live next to the existing ones in their package.
- Errors and logs never contain response bodies, tokens, keys, signed commands or personal fields (`value1..3`, account e-mail or name).
- Supervisor state is touched only by the `Run` goroutine. Tests drive handlers directly with the harness fake clock (`internal/gateway/harness_test.go`). No test sleeps on the real clock except the end-to-end tests in `internal/app`.
- Tests are table-driven where cases share a shape. Test names are `Test<Unit><Scenario>`.
- Commit messages are `<area>: <what>`, ending with the Co-Authored-By trailer.
- Doc comments explain why, not what, and follow the density of the surrounding code.

## Global Constraints

- Command deadlines are absolute from MQTT arrival: **30 s** for `LOCK`/`UNLOCK` and **10 s** for `OPEN`.
- Local retries stop **10 s** before the deadline (OPEN: **3 s**). Backoff is 0.5 s, 1 s, 2 s, then every 2 s. At most **one** cloud attempt is made per command.
- A request is retried only after `loqed.ErrUnreachable` (or an unusable bridge client). It is never retried after `ErrNoResponse`, after any bridge answer or after any cloud answer. Every attempt is signed afresh.
- `WebhookConfirm` = **30 s**. One more `/status` is read **60 s** after that if the command is still unresolved. After `MOTOR_STALL`, `/status` is read once **30 s** later.
- `/status` bolt data is applied only when no webhook changed the bolt in the last **5 min**, or when it shows the expected target. A read within **3 min** of a command or `GO_TO_STATE_*` that still shows the previous state is inconclusive: the bolt is left unchanged and `state_stale` is set.
- "Already there" grace: **5 s**. Gateway-source window: **60 s**. Cloud/bridge match window: **30 s**. Duplicate window: **10 s**.
- Key ids: a real key is **0..254**. **255**, `""`, `null`, absent or out of range mean no key → `nil`, and the event source is `unknown`.
- `command_status` (retained) is `{"command","id","status","via","attempts","error","received_at","updated_at"}`.
  - `status` is one of `pending`, `sending`, `sent`, `accepted`, `confirmed`, `failed`, `expired`, `superseded`.
  - `via` is `local`, `cloud` or `null`.
  - `error` is one of `unreachable`, `no_response`, `rejected`, `unauthorized`, `key_deleted`, `rate_limited`, `stalled`, `no_confirmation`, `offline`, or `null`.
- The command `id` is at most 64 characters, all printable.
- A token's `exp` is read from the JWT payload with no signature check. From 14 days before expiry: a daily warning, and a minted token is re-minted. Re-minting is still limited to once per hour.
- The cloud webhook route is `POST /cloud/<cloud_secret>/<lock-id>`. A numeric id that differs from the stored one gets 409.
- The global constraints of the gateway plan still apply, except those superseded above ("command max age 10 s", "webhook confirm 10 s").

## Review Focus

1. **A command is resent after the bridge may have acted.** For example, a connect that succeeded but timed out reading. Expected: no second actuation, either locally or via the cloud. Pinned by Task 7, "ErrNoResponse" tests.
2. **A burst of commands.** UNLOCK, LOCK, UNLOCK sent within 1 s while the bridge is down. Expected: exactly one actuation (the last UNLOCK), with the earlier ones published as `superseded`. Pinned by Task 7, "burst while unreachable".
3. **The bridge's Wi-Fi drops for about 8 s mid-command.** Expected: local retries succeed without a cloud command and without a failover storm, counting one HTTP failure per command. Pinned by Task 7, "recovers before cutoff".
4. **A cloud webhook copy arrives before the bridge copy.** Expected: one HA event, carrying the cloud `key_name`, not two events. Pinned by Task 6, "cloud first".
5. **A retained JSON `{"command":"OPEN"}` on reconnect.** Expected: it is ignored exactly like plain `OPEN`. Pinned by Task 4, retained JSON test.

---

## File Structure

```
flex.go                              + KeyID: lenient key_local_id decoding (0..254, else none)
bridge/events.go                     key_local_id via KeyID
cloud/webhook.go                     key_local_id via KeyID; personal fields stay undecoded
cloud/client.go                      ErrKeyDeleted on the ApiKey 404
internal/auth/expiry.go     (new)    TokenExpiry(token) from the JWT exp claim
internal/auth/auth.go                Resolver.KeyDeleted, Resolver.CheckExpiry
internal/model/model.go              CommandStatus, statuses/errors, ParseCommandMessage, State.TokenExpiresAt
internal/model/mapping.go            SourceFor (unknown/gateway/parsed); NormalizeKeyID removed
internal/hass/topics.go              CommandStatus topic
internal/hass/client.go              JSON commands with id, PublishCommandStatus (retained, cached, cleared)
internal/hass/discovery.go           Last command + Token expires sensors, signal % units
internal/gateway/supervisor.go       Timing constants, wake timer in Run, event source, publish token expiry
internal/gateway/statushint.go (new) /status hint rules and confirmation re-reads
internal/gateway/eventfeed.go  (new) duplicate drop against the latest event (event_dedup_window)
internal/gateway/pipeline.go   (new) command pipeline (Submit, delivery, confirmation)
internal/gateway/commands.go         removed (logic moves to pipeline.go)
internal/gateway/cloudhub.go         ErrKeyDeleted → token invalidation, no resend
internal/gateway/manager.go          DeliverCommand with id, DeliverCloudEvent(lockID, ev), BindCloudID
internal/store/store.go              LockRecord.CloudWebhookID, kept by Merge
internal/webhook/handler.go          per-lock cloud route, 409 on id mismatch
internal/webhook/urls.go             CloudURL(public, secret, lockID)
internal/app/app.go                  per-lock URL logs, expiry ticker, token expiry to supervisors
internal/app/app_test.go             realistic fake bridge, command_status end to end
README.md, addon/DOCS.md, addon/translations/en.yaml   documentation (spec 9)
```

---

### Task 1: Library — no-key markers and the deleted-key error

**Files:**
- Modify: `flex.go`, `flex_test.go`
- Modify: `bridge/events.go`, `bridge/events_test.go`
- Modify: `cloud/webhook.go`, `cloud/webhook_test.go`
- Modify: `cloud/client.go`, `cloud/client_test.go`

**Interfaces:**
- Produces:
  - `type loqed.KeyID struct` with `UnmarshalJSON` and `func (k *KeyID) Ptr() *int`. A nil receiver gives nil.
  - `bridge.StateReachedEvent.KeyLocalID` / `GoToStateEvent.KeyLocalID` and `cloud.WebhookEvent.KeyLocalID` are `*int` holding a real key 0..254 or nil.
  - `var cloud.ErrKeyDeleted`.

Behavior:

```
loqed.KeyID
    UnmarshalJSON
        On a number or numeric string 0..254
            - Ptr returns that id
        On 255, "", null, a non-numeric string or a value outside 0..254
            - Ptr returns nil
            - returns no error (a bad key id never rejects the event)
        On an object or array
            - returns an error
bridge.ParseEvent
    On key_local_id 255, "", null or absent
        - the state and go-to events carry KeyLocalID nil
    On key_local_id "0" or 0
        - KeyLocalID points to 0 (key 0 is real)
cloud.ParseWebhook
    On the observed state-reached payload (key_account_email, value1..value3, numeric lock_id, key_local_id "1")
        - LockID is "6148", KeyLocalID points to 1
        - no decoded field contains the e-mail or the value1..value3 texts
    On key_local_id ""
        - KeyLocalID is nil
cloud.Client.Command
    On 204
        - returns nil
    On 404 whose body contains "No query results for model [App\\Models\\ApiKey]"
        - returns an error matching cloud.ErrKeyDeleted (and still matching *loqed.APIError)
    On any other 404
        - returns *loqed.APIError, not ErrKeyDeleted
```

- [ ] **Step 1:** Write the tests above, one per effect, in the existing table-driven files. Use the observed payloads from spec 2.2 with personal values replaced by obviously fake ones.
- [ ] **Step 2:** Run `go test ./ ./bridge ./cloud` and confirm the new cases fail.
- [ ] **Step 3:** Implement:
  - `KeyID` in `flex.go`, reusing `scalarText`.
  - Use it for `key_local_id` in both raw structs. Drop `bridge.keyID` and the inline range check in `cloud.ParseWebhook`.
  - `ErrKeyDeleted` in `cloud`, detected inside `Command` with `errors.As` on the `*loqed.APIError` from `get` (status 404 plus the `App\Models\ApiKey` marker). Return both errors joined so callers can match either.
- [ ] **Step 4:** Run `go test -race ./...` and confirm it passes. `internal/model.NormalizeKeyID` still compiles; it is removed in Task 3.
- [ ] **Step 5:** Commit `library: treat key 255, "" and null as no key; detect deleted token keys`.

---

### Task 2: Token expiry, expiry checks and deleted-key re-mint

**Files:**
- Create: `internal/auth/expiry.go`, `internal/auth/expiry_test.go`
- Modify: `internal/auth/auth.go`, `internal/auth/auth_test.go`
- Modify: `internal/gateway/cloudhub.go`, `internal/gateway/cloudhub_test.go`

**Interfaces:**
- Consumes: `cloud.ErrKeyDeleted` (Task 1).
- Produces:
  - `func auth.TokenExpiry(token string) (time.Time, bool)`.
  - `const auth.ExpiryWarning = 14 * 24 * time.Hour`.
  - `func (r *Resolver) Expiry() (time.Time, bool)` for the token currently resolvable without network I/O (the configured token or the cached minted token; false if unknown).
  - `func (r *Resolver) CheckExpiry(ctx context.Context) (reminted bool, err error)`.
  - `func (r *Resolver) KeyDeleted(ctx context.Context, token string) (string, error)`.
  - `TokenSource` gains `KeyDeleted(ctx, token string) (string, error)`.

Behavior:

```
auth.TokenExpiry
    On a three-part JWT whose base64url payload has a numeric exp
        - returns time.Unix(exp) in UTC and true
    On a non-JWT string, bad base64, bad JSON or a missing/non-numeric exp
        - returns false (never an error that could carry the token)
Resolver.CheckExpiry
    When the token expires in more than 14 days (or the expiry is unknown)
        - logs nothing and mints nothing
    When a configured cloud_token expires within 14 days
        - logs one warning per 24 h naming the expiry time (never the token)
        - never mints
    When a minted token expires within 14 days
        - mints a replacement (subject to the persisted hourly limit)
        - returns reminted=true
    When minting is throttled or fails
        - logs the warning and returns the error; the old token stays in use
Resolver.KeyDeleted
    With a configured cloud_token
        - returns an error explaining that the token's lock key was deleted in the LOQED app and a new token is needed
    With a minted token equal to the rejected one
        - mints a new token (hourly limit applies) and returns it
CloudHub.Command
    On cloud.ErrKeyDeleted
        - returns an error matching cloud.ErrKeyDeleted
        - never resends the command (with any token)
        - asks the token source for a replacement via KeyDeleted, so the next call uses it
    On ErrUnauthorized
        - keeps today's behavior: one resend with a replacement token (a rejected request did nothing)
```

- [ ] **Step 1:** Write the tests. Build JWTs in the test from a fixed header and a payload with `exp`; there is no signature check, so the signature part is any string.
- [ ] **Step 2:** Run `go test ./internal/auth ./internal/gateway` and confirm the new cases fail.
- [ ] **Step 3:** Implement `expiry.go`, then `Resolver.Expiry` / `CheckExpiry` / `KeyDeleted` (the daily-warning timestamp is kept in memory), then the `CloudHub.Command` branch. Update the `fakeTokens` test double.
- [ ] **Step 4:** Run `go test -race ./...` and confirm it passes.
- [ ] **Step 5:** Commit `auth: read token expiry, re-mint before expiry and after a deleted key`.

---

### Task 3: Model — command status, JSON commands, event sources

**Files:**
- Modify: `internal/model/model.go`, `internal/model/mapping.go`, `internal/model/model_test.go`
- Modify: callers of `model.Source`, `model.NormalizeKeyID` and `model.FailOther` in `internal/gateway` (mechanical; these become pass-throughs or `FailRejected`)

**Interfaces:**
- Produces:
  - `type CommandStatusValue string` with constants `StatusPending`, `StatusSending`, `StatusSent`, `StatusAccepted`, `StatusConfirmed`, `StatusFailed`, `StatusExpired`, `StatusSuperseded`, and `var CommandStatusValues []CommandStatusValue` in that order.
  - `type Via string` (`ViaLocal`, `ViaCloud`).
  - Failure classes: `FailUnreachable`, `FailNoResponse`, `FailRejected`, `FailUnauthorized`, `FailKeyDeleted`, `FailRateLimited`, `FailStalled`, `FailNoConfirmation`, `FailOffline`, and `FailExpired` (used only as the `command_failed` event error for `expired`). `FailOther` is removed.
  - `type CommandStatus struct { Command Command "command"; ID *string "id"; Status CommandStatusValue "status"; Via *Via "via"; Attempts int "attempts"; Error *string "error"; ReceivedAt time.Time "received_at"; UpdatedAt time.Time "updated_at" }`. Times are UTC, truncated to milliseconds.
  - `func ParseCommandMessage(payload []byte) (c Command, id string, err error)`.
  - `State.TokenExpiresAt *time.Time` with JSON `token_expires_at,omitempty`.
  - `func SourceFor(eventType string, key *int, gateway bool) string`, plus the constant `SourceUnknown = "unknown"`.

Behavior:

```
ParseCommandMessage
    On "LOCK", " unlock ", "Open"
        - returns the command and an empty id
    On {"command":"UNLOCK","id":"auto-42"}
        - returns UNLOCK and "auto-42"
    On JSON without id, or with "id": null
        - returns the command and an empty id
    On an unknown command (plain or JSON), malformed JSON, or JSON whose command is not a string
        - returns an error naming the accepted forms
    On an id longer than 64 characters or containing non-printable characters
        - returns an error (the command is not executed with a mangled id)
CommandStatus JSON
    With no id, no via and no error
        - marshals "id":null, "via":null, "error":null (the keys are always present)
SourceFor
    With key nil
        - returns "unknown" for every event type, including *_REMOTE_* and touch events
    With gateway true and a key
        - returns "gateway"
    Otherwise
        - TWIST_ASSIST → twist_assist, INSTANTOPEN → instant_open, TOUCH → touch, REMOTE → remote, anything else → other
        - GO_TO_STATE_MANUAL_LOCK_REMOTE_NIGHT_LOCK → remote (no "manual" source exists any more)
State JSON
    With TokenExpiresAt nil
        - omits token_expires_at
```

- [ ] **Step 1:** Write the tests above, replacing the old `Source` and `NormalizeKeyID` tests.
- [ ] **Step 2:** Run `go test ./internal/model` and confirm the new cases fail.
- [ ] **Step 3:** Implement. Remove `Source`, `NormalizeKeyID` and `FailOther`. In `internal/gateway`:
  - `recordEvent` uses `SourceFor(eventType, key, false)`; the gateway flag is wired in Task 6.
  - `NormalizeKeyID(x)` becomes `x`.
  - `FailOther` becomes `FailRejected`.
  - Gateway tests that asserted `manual` or `ble` sources are updated to the new values.
- [ ] **Step 4:** Run `go test -race ./...` and confirm it passes.
- [ ] **Step 5:** Commit `model: command status, JSON command payloads, unknown source for keyless events`.

---

### Task 4: MQTT — command_status topic, JSON commands, new sensors

**Files:**
- Modify: `internal/hass/topics.go`, `internal/hass/client.go`, `internal/hass/client_test.go`
- Modify: `internal/hass/discovery.go`, `internal/hass/discovery_test.go`, `internal/hass/testdata/*.json` (golden, regenerated with `-update`)

**Interfaces:**
- Consumes: `model.ParseCommandMessage`, `model.CommandStatus`, `model.CommandStatusValues` (Task 3).
- Produces:
  - `func (Topics) CommandStatus(id string) string` returning `<base>/<id>/command_status`.
  - `hass.Command` gains an `ID string` field.
  - `func (c *Client) PublishCommandStatus(lockID string, s model.CommandStatus) error`.

Behavior:

```
Client.onCommand
    On a non-retained JSON command with an id
        - queues Command{LockID, Command, ID, At}
    On a retained message, plain or JSON
        - queues nothing and logs a warning
    On an invalid payload
        - queues nothing and logs a warning without the payload
Client.PublishCommandStatus
    On a connected client
        - publishes the JSON retained, QoS 1, on <base>/<id>/command_status
    On reconnect
        - republishes the latest cached status of every lock
    When a lock is removed (SetLocks removed ids)
        - clears its retained command_status together with state and availability
DiscoveryPayload
    Last command component
        - sensor "Last command", diagnostic, device_class enum with options = CommandStatusValues
        - state_topic command_status, value_template {{ value_json.status }}
        - json_attributes from command_status: command, id, via, attempts, error, updated_at
    Token expires component
        - sensor "Token expires", diagnostic, device_class timestamp, from token_expires_at (None when absent)
    Signal sensors
        - Wi-Fi and BLE sensors carry unit_of_measurement "%"
        - the BLE sensor is unavailable while ble_strength is -1: its own availability list repeats the gateway status and lock availability topics and adds the state topic with a template, with availability_mode all
```

- [ ] **Step 1:** Write the client tests against the in-process broker (`internal/testutil`), and the discovery assertions.
- [ ] **Step 2:** Run `go test ./internal/hass` and confirm the new cases fail.
- [ ] **Step 3:** Implement. Command statuses are cached per lock like `states`, under the same `retainMu` ordering rule. Regenerate the golden file with `go test ./internal/hass -run TestDiscovery -update` and review its diff.
- [ ] **Step 4:** Run `go test -race ./...` and confirm it passes. The `internal/app` forwarder passes `ID` through in Task 7, so the zero value is fine until then.
- [ ] **Step 5:** Commit `hass: command_status topic, JSON commands, last-command and token-expiry sensors`.

---

### Task 5: Gateway — `/status` as a hint, confirmation timing, availability recovery, webhook-count warning

**Files:**
- Create: `internal/gateway/statushint.go`, `internal/gateway/statushint_test.go`
- Modify: `internal/gateway/supervisor.go` (Timing), `internal/gateway/local.go`, `internal/gateway/cloudevents.go`
- Modify: the existing gateway tests whose timings change (10 s → 30 s)

**Interfaces:**
- Produces:
  - `Timing` fields:
    - `WebhookConfirm` (default 30 s) and `StatusRecheck` (60 s);
    - `StatusEventWindow` (5 min) and `StatusMoveWindow` (3 min);
    - `CommandDeadline` (30 s), `OpenDeadline` (10 s), `LocalCutoff` (10 s), `OpenLocalCutoff` (3 s);
    - `AlreadyThere` (5 s), `GatewayWindow` (60 s), `AutoLatchWindow` (5 s), `DuplicateWindow` (10 s).

    `CommandMaxAge` is removed in Task 7.
  - The supervisor tracks `lastBoltEventAt` (when a webhook last changed the bolt) and a movement `{from, target, at}`, set by gateway commands (Task 7) and by `GO_TO_STATE_*` events.
  - `func (s *Supervisor) applyStatusHint(now time.Time, st *bridge.Status) (matchedTarget bool)` replaces the bolt part of `applyStatus`.

Behavior:

```
Supervisor.applyStatusHint
    When no webhook changed the bolt in the last 5 min and no movement is pending
        - applies the bolt state and lock state
        - clears state_stale
    When a webhook changed the bolt less than 5 min ago and /status disagrees
        - leaves the bolt and lock state unchanged
        - does not set state_stale
    When a movement started less than 3 min ago and /status shows its target
        - applies the bolt state, clears the movement and state_stale
        - returns matchedTarget = true
    When a movement started less than 3 min ago and /status still shows the previous state
        - leaves the bolt and lock state unchanged
        - sets state_stale until a webhook or a later read resolves it
    Always
        - applies battery, voltage, Wi-Fi, BLE and lock_online
local confirmation reads
    When a GO_TO_STATE_* arrives and no STATE_CHANGED_* reaching its target follows
        - reads /status 30 s after it
        - reads /status once more 60 s after that read if still unresolved, and not again
    After MOTOR_STALL
        - reads /status once, 30 s later
    When STATE_CHANGED_* reaching the target arrives first
        - reads nothing
availability
    After an online event with ble_strength -1
        - any later state, battery or signal event (bridge or cloud) with ble_strength not -1, or without ble_strength, marks the lock online again
ensureWebhook
    When the bridge lists more than 3 webhooks besides ours
        - logs one warning (rate-limited like other warnings) that each webhook target delays events and /status
    With 3 or fewer
        - logs nothing
```

- [ ] **Step 1:** Write `statushint_test.go` with the harness: one test per effect. Update existing tests that assumed 10 s confirmation, or that `/status` always wins, to the new rules. These are behavior changes demanded by spec 5.5, not regressions.
- [ ] **Step 2:** Run `go test ./internal/gateway` and confirm the new cases fail.
- [ ] **Step 3:** Implement:
  - The hint rules go in `statushint.go`.
  - `applyStatus` keeps the non-bolt fields and delegates the bolt to the hint.
  - `tickLocal`'s confirmation step reads twice (30 s, then 60 s later).
  - Availability recovery goes in `onBridgeEvent` / `onCloudEvent`.
  - The webhook count check goes in `ensureWebhook`.
- [ ] **Step 4:** Run `go test -race ./...` and confirm it passes.
- [ ] **Step 5:** Commit `gateway: treat /status as a hint, confirm after 30 s, recover availability`.

---

### Task 6: Gateway — event feed (duplicates, cloud-first matching, auto-latch, sources)

**Files:**
- Create: `internal/gateway/eventfeed.go`, `internal/gateway/eventfeed_test.go`
- Modify: `internal/gateway/supervisor.go` (`recordEvent`), `internal/gateway/local.go` (`onBridgeEvent`), `internal/gateway/cloudevents.go`
- Modify: `internal/gateway/cloudevents_test.go`

**Interfaces:**
- Consumes: `model.SourceFor` (Task 3) and the Timing windows (Task 5).
- Produces:
  - `func (s *Supervisor) isDuplicate(eventType string, key *int, now time.Time) bool` (revised: compares with the latest event only; no per-feed lists).
  - `func (s *Supervisor) gatewayActive(now time.Time) bool`: a command is in flight or one was sent within `GatewayWindow`. It is backed by `lastCommandSentAt`, set by Task 7.

Behavior:

```
event feed (local mode, bridge webhook registered)
    On a cloud state or go-to event with no bridge copy yet
        - (revised 2026-10-06) publishes it at once: the first copy from either feed wins
    On a later copy of the latest event (same event_type and key, any feed) within event_dedup_window
        - drops it; a cloud copy may still add key_name to the state document (no second event)
duplicates (revised 2026-10-06: only the latest event is compared; window configurable, default 10 s, max 30 s)
    On the same event_type and key as the latest event within the window
        - drops the delivery (no state publish, no event)
    On a repeat after another event, or outside the window
        - processes it
automatic latch (revised 2026-10-06: no filtering, the gateway is a faithful bridge)
    On STATE_CHANGED_LATCH with no key shortly after STATE_CHANGED_OPEN
        - sets lock UNLOCKED and bolt day_lock
        - publishes an "unlocked" event with source unknown
event source
    On an event without a key
        - source "unknown"
    On an event with the gateway's local_id while gatewayActive
        - source "gateway"
    On an event with the gateway's local_id with no recent gateway command
        - source parsed from the event type (for example "remote")
cloud mode
    On cloud events
        - duplicates are dropped; there is no matching, since there is no bridge feed
```

- [ ] **Step 1:** Write the tests. Existing enrichment tests stay and must still pass.
- [ ] **Step 2:** Run `go test ./internal/gateway` and confirm the new cases fail.
- [ ] **Step 3:** Implement:
  - Keep small fixed-size recent-event lists per feed, pruned by time; no unbounded growth.
  - Held cloud copies are flushed (dropped) from `tick`.
  - `recordEvent` takes the feed and computes the source with `SourceFor(eventType, key, key == local_id && gatewayActive(now))`.
- [ ] **Step 4:** Run `go test -race ./...` and confirm it passes.
- [ ] **Step 5:** Commit `gateway: drop duplicate events, match cloud-first copies, hide the automatic latch`.

---

### Task 7: Gateway — command pipeline

**Files:**
- Create: `internal/gateway/pipeline.go`, `internal/gateway/pipeline_test.go`
- Delete: `internal/gateway/commands.go`; move its still-valid tests from `commands_test.go` into `pipeline_test.go` and delete the rest
- Modify:
  - `internal/gateway/supervisor.go`: `CommandMsg`, the Run wake timer, the `Publisher.PublishCommandStatus` dependency, removal of `CommandMaxAge`.
  - `internal/gateway/manager.go`: `DeliverCommand(lockID, c, id, at)`.
  - `internal/gateway/local.go` and `cloudmode.go`: confirmation hooks.
  - `internal/gateway/harness_test.go`: the fake publisher records command statuses; a sub-second `advance`.
  - `internal/app/app.go`: forward `ID`.

**Interfaces:**
- Consumes:
  - `model.CommandStatus` (Task 3);
  - `hass.Client.PublishCommandStatus` (Task 4);
  - the movement tracking and status hint (Task 5);
  - `gatewayActive` / `lastCommandSentAt` (Task 6);
  - `cloud.ErrKeyDeleted` (Task 1).
- Produces:
  - `CommandMsg{Command model.Command; ID string; At time.Time}`.
  - `Publisher.PublishCommandStatus(lockID string, s model.CommandStatus) error`.
  - Unexported `commandPipeline`, owned by the Supervisor, with the methods:
    - `Submit(now time.Time, c model.Command, id string, receivedAt time.Time)`;
    - `step(ctx context.Context, now time.Time)`, which is called from `tick` and right after `Submit`;
    - `nextWake() time.Time`;
    - `onGoTo(now time.Time, target loqed.BoltState, key *int)`;
    - `onReached(now time.Time, bolt loqed.BoltState, jammed bool, key *int)`;
    - `onStatus(now time.Time, matchedTarget bool)`;
    - `onPoll(now time.Time, bolt loqed.BoltState, final bool)`.
  - `Supervisor.Run` resets a `time.Timer` to `nextWake()` after every message and tick, so 0.5 s retries fire on time.

Behavior (spec 5.8; one test per effect):

```
CommandPipeline (internal/gateway)
    Submit(command, id, receivedAt)
        When nothing is pending or in flight
            - publishes command_status pending with the given id and received_at
            - starts delivery immediately (status sending, attempts 1)
        When a different command is pending and not yet written
            - replaces the pending command
            - publishes superseded for the replaced command
        When an equal command is pending or in flight
            - sends nothing new
            - republishes the existing command's status under the new id (an empty id keeps the old one)
        When a command is in flight (written, not resolved)
            - keeps the new command pending until the in-flight one resolves (confirmed, failed, expired)
            - never retries the in-flight command if it ends not-delivered
        When the lock is offline
            - publishes failed with error offline
            - publishes a command_failed event with error offline
    deliverLocal
        On bridge 200
            - publishes sent with via local and the attempt count
            - sets the lock state to LOCKING/UNLOCKING/OPENING unless the lock already is in the target state
            - starts the 30 s confirmation window and records the movement {from, target}
        On ErrUnreachable (refused, no route, connect timeout)
            - signs every retry with a fresh timestamp (the bridge client is called again; no cached request)
            - retries after 0.5 s, 1 s, 2 s, then every 2 s
            - stops local retries 10 s before the deadline (OPEN: 3 s)
            - then sends once via the cloud when the lock has cloud access
            - publishes failed with error unreachable when there is no cloud access (the cloud attempt itself was unreachable or no token exists)
            - counts one bridge HTTP failure per command, not per attempt
        When the bridge recovers before the cutoff
            - the next local attempt delivers; no cloud command is sent
        On ErrNoResponse or any non-2xx bridge answer other than unauthorized
            - never resends locally or via the cloud
            - publishes failed with error no_response or rejected
            - still runs confirmation (webhooks, then the /status hint); a later confirmation publishes confirmed
        On ErrUnauthorized
            - sends once via the cloud when possible
            - triggers a cache refresh after the command resolves
        When the deadline passes before any request was written
            - publishes expired and a command_failed event with error expired
            - sends nothing afterwards
        When the bridge client cannot be built
            - skips local attempts and goes straight to the one cloud attempt
    deliverCloud (cloud mode, or after local attempts)
        On 204
            - publishes sent with via cloud
            - schedules the cloud confirmation poll (5.6) and, with cloud webhooks, waits for them
        On cloud.ErrKeyDeleted
            - publishes failed with error key_deleted
            - refreshes the lock record after the command resolves (the cached local key is dead too)
        On ErrNoResponse or 5xx
            - never resends
            - publishes failed with error no_response
            - schedules the confirmation poll
        On ErrRateLimited
            - publishes failed with error rate_limited
        On ErrUnauthorized (after the hub's single token replacement)
            - publishes failed with error unauthorized
    confirm
        On GO_TO_STATE toward the target with the gateway's key
            - publishes accepted
        On STATE_CHANGED reaching the target (any key)
            - publishes confirmed
            - starts the next pending command
        On accepted while the lock already is in the target state and no STATE_CHANGED within 5 s
            - publishes confirmed
        On MOTOR_STALL with the gateway's key
            - publishes failed with error stalled
            - sets the lock state to JAMMED
        On no STATE_CHANGED within 30 s (local)
            - reads /status first; a read showing the target publishes confirmed
            - otherwise publishes failed with error no_confirmation and sets state_stale
        On a cloud confirmation poll showing the target
            - publishes confirmed
        On the final cloud confirmation poll (after its one retry) still not showing the target, or 30 s without any confirmation in cloud mode
            - publishes failed with error no_confirmation
    burst while unreachable
        On UNLOCK, LOCK, UNLOCK within 1 s with the bridge refusing connections, then recovering
            - exactly one bridge command (UNLOCK) is delivered
            - command_status shows superseded for UNLOCK and LOCK, then sent/confirmed for the last UNLOCK
```

Algorithm (language-agnostic pseudocode; the timing is the tricky part):

```
Submit(c):
    if mode == offline: fail(c, offline); return
    latest = pending or (active if active not terminal)
    if latest and latest.cmd == c.cmd: coalesce(latest, c.id); return
    if pending: supersede(pending); pending = none
    if active and not active.written: supersede(active); active = none
    if active and active.cmd == c.cmd: coalesce(active, c.id); return
    if active: pending = c; publish(pending, status=pending)
    else: active = c; publish(active, status=pending); step()

step(now):
    if active is terminal: active = pending; pending = none
    if pending and now >= pending.deadline: expire(pending); pending = none
    if not active or active.written or now < active.nextAttempt: return
    if now >= active.deadline: expire(active); return
    cutoff = active.deadline - (OPEN ? 3s : 10s)
    if mode == local and bridge usable and not active.skipLocal and now < cutoff:
        result = bridge.Command(ctx with min(RequestTimeout, deadline - now))
        active.attempts += 1
        if ok: sent(local); return
        if unreachable:
            if active.attempts == 1: count one HTTP failure
            delay = [0.5s, 1s, 2s][attempts-1] or 2s
            if now + delay < cutoff: active.nextAttempt = now + delay; return
            # otherwise fall through to the cloud on this same step
        else if unauthorized: active.refreshAfter = true   # fall through to the cloud
        else: written; fail(no_response or rejected); confirm anyway; return
    if active.cloudTried: fail(unreachable); return
    active.cloudTried = true
    cloud.Command(ctx with deadline) → sent(cloud) | fail(class) (+ confirmation poll on no_response)
```

The `active`/`pending` slots, the timers and the status publications all live on the supervisor goroutine. Every blocking call is bounded by the command deadline and `RequestTimeout`.

- [ ] **Step 1:** Write `pipeline_test.go`, one test per effect, grouped by scenario. Add `harness.advanceBy(d)` for sub-second steps. Run the burst and the recovery cases with `fakeBridge.commandErrs`.
- [ ] **Step 2:** Run `go test ./internal/gateway` and confirm the new cases fail.
- [ ] **Step 3:** Implement `pipeline.go`:
  - Wire `tick` → `step`, and route events and polls into `onGoTo` / `onReached` / `onStatus` / `onPoll`.
  - Add the Run wake timer.
  - Add `PublishCommandStatus` to `Publisher` and implement it in `hass.Client` (already there) and the test fakes.
  - Pass the command id from `hass.Command` through `app.forwardCommands`, `Manager.DeliverCommand` and `CommandMsg`.
  - Delete `commands.go`.
- [ ] **Step 4:** Run `go test -race ./...` and confirm it passes. Also run `gofmt -l .` and the pinned golangci-lint.
- [ ] **Step 5:** Commit `gateway: command pipeline with latest-wins, safe retries and webhook confirmation`.

---

### Task 8: Per-lock cloud webhook URLs

**Files:**
- Modify: `internal/store/store.go`, `internal/store/store_test.go`
- Modify: `internal/gateway/manager.go`, `internal/gateway/supervisor.go`, `internal/gateway/cloudevents_test.go`
- Modify: `internal/webhook/handler.go`, `internal/webhook/urls.go`, `internal/webhook/handler_test.go`
- Modify: `internal/app/app.go`, `internal/app/app_test.go`

**Interfaces:**
- Produces:
  - `store.LockRecord.CloudWebhookID string` (`cloud_webhook_id,omitempty`). `store.Merge` keeps the old value when the fresh one is empty.
  - `gateway.ErrCloudIDMismatch`.
  - `Manager.DeliverCloudEvent(lockID string, ev cloud.WebhookEvent) error`, which binds or verifies the id through `Supervisor.BindCloudID(numericID string) error`. The supervisor guards this with `mu` and persists through a new `Deps.SaveCloudWebhookID func(lockID, id string) error`.
  - `webhook.CloudURL(publicBase, secret, lockID string) string`.
  - `webhook.Sink.DeliverCloudEvent(lockID string, ev cloud.WebhookEvent) error`.

Behavior:

```
POST /cloud/<secret>/<lock-id>
    With a wrong secret
        - 404 (constant-time compare)
    With an unknown lock id
        - 404
    On the first event for a lock
        - stores the body's numeric lock_id as cloud_webhook_id (persisted in the cache)
        - delivers the event to that lock's supervisor; 200
    On a later event with the same numeric id
        - 200
    On a later event with a different numeric id
        - 409 and one warning naming both lock ids (no personal data)
    On POST /cloud/<secret> without a lock id
        - 404 (the old single URL is gone)
store.Merge
    On a refresh whose record lacks cloud_webhook_id
        - keeps the cached cloud_webhook_id
app startup with webhook.public_url
    - logs one line per selected lock with its own cloud webhook URL and the lock name
```

- [ ] **Step 1:** Write the tests.
- [ ] **Step 2:** Run `go test ./internal/...` and confirm the new cases fail.
- [ ] **Step 3:** Implement. The supervisor routes by the path id; `cloud.WebhookEvent.LockID` is only checked against the binding, never used for routing.
- [ ] **Step 4:** Run `go test -race ./...` and confirm it passes.
- [ ] **Step 5:** Commit `webhook: one cloud webhook URL per lock`.

---

### Task 9: Wiring, realistic fakes, end to end, documentation

**Files:**
- Modify: `internal/app/app.go`, `internal/app/app_test.go`
- Modify: `internal/gateway/supervisor.go` (`Deps.TokenExpiry func() (time.Time, bool)`, published in the state document)
- Modify: `README.md`, `addon/DOCS.md`, `addon/translations/en.yaml` (if option texts change)
- Modify: `docs/superpowers/plans/2026-10-04-loqed-mqtt-gateway.md` (Task 16 checklist: V1, V3–V7 already recorded in spec 2.5; V9 added)

**Interfaces:**
- Consumes: everything above.

Behavior:

```
app
    On startup and every hour
        - calls Resolver.CheckExpiry; after a re-mint, refreshes all lock records (RefreshAll) so cloud commands and key checks use the new token's key
    State documents
        - carry token_expires_at when the expiry of the token in use is known
fake bridge (internal/app/app_test.go)
    On /to_lock
        - always answers 200 "Message resent to the lock"
        - acts only on a valid signature with a timestamp less than 60 s old
        - posts GO_TO_STATE_* with the gateway's key after a short delay, then STATE_CHANGED_* (test-sized delays)
    On /status
        - returns the bolt state only after the STATE_CHANGED_* webhook was delivered (lag)
end to end
    On UNLOCK over MQTT
        - command_status goes pending → sending → sent → accepted → confirmed
        - the lock state goes UNLOCKING → UNLOCKED
        - exactly one /to_lock call
    On {"command":"LOCK","id":"e2e"} with a bad key secret
        - the fake bridge answers 200 but never acts
        - command_status ends failed with error no_confirmation and id "e2e"
```

Documentation (spec 9) is not behavior; it is checked by reading. Add:
- the firewall rule (bridge → gateway webhook port);
- removing stale bridge webhooks;
- deleting the gateway's key in the LOQED app to revoke access (revoking the token is not enough);
- the `command_status` topic and the JSON command payload;
- token expiry and re-minting;
- registering each lock's own cloud webhook URL;
- recalibrating the lock if UNLOCK opens the door.

- [ ] **Step 1:** Write the app tests (use short `Timing` overrides through the existing app options).
- [ ] **Step 2:** Run `go test ./internal/app` and confirm the new cases fail.
- [ ] **Step 3:** Implement the wiring, the fake bridge realism and the documentation.
- [ ] **Step 4:** Run the full gate: `gofmt -l .` (empty), `go vet ./...`, `go test -race ./...`, `go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.14.0 run ./...`, and `docker build --target standalone .`.
- [ ] **Step 5:** Commit `app: token expiry, realistic bridge fake, command status end to end, docs`.

---

## Spec coverage

| Spec item | Task |
|---|---|
| 2.1 `/to_lock` acks everything, fresh signature per attempt | 7, 9 |
| 2.1 observed timings, keyless markers, sequential webhook delivery | 1, 5, 6, 9 |
| 2.2 cloud 204, token key, ApiKey 404, JWT exp, key lifecycle | 1, 2, 7 |
| 2.2 cloud webhook payloads, numeric lock id, duplicates, cloud-first | 1, 6, 8 |
| 2.3 `remember: false` | done (947fb4f) |
| 4.1 / 4.2 KeyLocalID rules, personal fields undecoded | 1 |
| 5.2 token lifetime, re-mint rules, key_deleted | 2, 7, 9 |
| 5.3 `cloud_webhook_id` | 8 |
| 5.4 per-lock cloud URL log line | 8 |
| 5.5 WebhookConfirm 30 s, `/status` hint, webhook-count warning, availability recovery | 5 |
| 5.7 sources, auto-latch, matching in either order, duplicates | 3, 6 |
| 5.8 command pipeline | 3, 4, 7 |
| 6.1 `command_status`, JSON command, `token_expires_at` | 3, 4, 9 |
| 6.2 Last command, Token expires, signal `%` | 4 |
| 7 per-lock cloud route, 409 | 8 |
| 9 documentation additions | 9 |
| 10 mock realism, pipeline tests, manual V2/V8/V9 | 7, 9, gateway plan Task 16 |

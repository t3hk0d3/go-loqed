# loqed-mqtt rev 2.2: Cloud Webhooks over MQTT Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Accept LOQED cloud webhook bodies on a per-lock MQTT topic, so a Home Assistant automation with a Nabu Casa webhook trigger can feed cloud webhooks without a reverse proxy.

**Architecture:**
- First, `internal/hass` is split into `internal/mqtt` (MQTT client and the gateway's own topics) and `internal/mqtt/hass-discovery` (package `hassdiscovery`, Home Assistant discovery). The client gets discovery through a small interface, so the MQTT layer knows nothing about Home Assistant.
- `internal/mqtt` subscribes to `<base>/+/cloud_webhook` when the feature is enabled. It filters out retained, unknown-lock and oversized messages, and hands the raw body plus the real lock id to the app over a channel.
- The app forwards each body to one new gateway entry point, `Manager.DeliverCloudWebhook`. That function decodes and routes the body exactly as the HTTP `/cloud/<secret>/<lock-id>` route does. The HTTP route is refactored to call the same function, so both inputs share the binding rules.

**Tech Stack:** Go 1.27, `log/slog`, paho MQTT (only in `internal/mqtt`), in-process mochi broker for tests (`internal/testutil`).

**Spec:** `docs/superpowers/specs/2026-10-04-loqed-mqtt-gateway-design.md`, rev 2.2. The relevant sections:
- the rev 2.2 header;
- 2.2 (payload shapes and the documentation discrepancy);
- 5.1 (`mqtt.cloud_webhooks`);
- 5.4 step 6;
- 5.5 ("cloud webhooks configured");
- 6.1 (`cloud_webhook` topic and the input rules);
- 7 (shared decode-and-route);
- 9 (DOCS recipe);
- 10;
- V10.

**Already done (not part of this plan):** the cloud webhook test fixtures were switched to the real payload shapes in `31d7e70`.

## Styleguide

The project styleguide is `CLAUDE.md` (Conventions, Safety invariants, Secrets) together with `.golangci.yml` (golangci-lint v2.14.0; gofmt). Every task follows it. The rules that matter most here:

- `internal/mqtt` is the only package that imports the MQTT library; it owns the gateway's `<base>/…` topics and knows nothing about Home Assistant. `internal/mqtt/hass-discovery` (package `hassdiscovery`) owns discovery documents, the discovery topic and HA's birth topic; it imports `internal/mqtt`, never the MQTT library. `internal/gateway` is the only package that knows lock modes and the cloud-id binding. `internal/app` only wires them together.
- Errors are wrapped with `%w` around existing sentinels: `loqed.ErrInvalidPayload`, `gateway.ErrUnknownLock`, `gateway.ErrCloudIDMismatch` and `gateway.ErrBusy`. Callers branch with `errors.Is`.
- Cloud webhook bodies are never logged, at any level. Log lines carry `lock_id`, the topic and, for a mismatch, the numeric `cloud_lock_id`, and nothing else from the payload.
- Tests:
  - stdlib `testing`, named `Test<Unit><Scenario>`, table-driven where cases share a shape;
  - MQTT tests use `internal/testutil` (in-process broker);
  - no real sleeps except bounded waits in broker and end-to-end tests.
- Commits: `<area>: <what>`, ending with the Co-Authored-By trailer.
- Doc comments explain why, following the density of the surrounding code.

## Global Constraints

- Topic: `<base>/<id>/cloud_webhook`, where `<id>` is the same topic id as in `<base>/<id>/command` (`mqtt.TopicID(lockID)`). Subscription: `<base>/+/cloud_webhook`, QoS 1, re-established on every reconnect.
- Subscribed only when `mqtt.cloud_webhooks` is `true`. The default is `false`.
- "Cloud webhooks configured" (the gateway's `Deps.CloudWebhooks`) = `webhook.public_url != ""` **or** `mqtt.cloud_webhooks`.
- Retained `cloud_webhook` messages are ignored with a warning. Messages for an unknown topic id are ignored with a warning. Payloads over **64 KiB** are dropped with a warning.
- Decode and routing are identical for HTTP and MQTT:
  - the topic or path id selects the lock;
  - the body's `lock_id` is bound as `cloud_webhook_id` on first use;
  - a mismatch is `gateway.ErrCloudIDMismatch`.
- The MQTT path has no reply. Every rejection is a log line:
  - mismatch: warn;
  - invalid payload: warn;
  - full queue: warn;
  - unknown lock: warn.
  Repeated identical warnings are rate-limited by the existing logger.
- The HTTP route's status codes do not change: 404, 400, 409, 503 and 200 as today.
- A payload is never logged.
- Relays forward the LOQED body unchanged (no field filtering). The gateway still never decodes or logs the personal fields.
- The add-on option is `mqtt.cloud_webhooks: bool?`. Its translation has a name and a one-line description.

## Review Focus

1. **A retained `cloud_webhook` message left on the broker** (a relay configured with `retain: true`). It must never be applied, at first connect or on any reconnect, and the warning must say to publish without retain. Test: Task 3, `TestCloudWebhookRetainedIgnored`, covering the initial connect and a reconnect.
2. **Feature disabled but someone publishes to the topic.** Nothing reaches the gateway, because there is no subscription. Test: Task 3, `TestCloudWebhookNotSubscribedWhenDisabled`.
3. **The relay posts one lock's events to another lock's topic** (wrong trigger id in a shared automation). The first event binds; a later different numeric id is dropped with a warning naming both ids, and the lock's state is unchanged. Test: Task 4, `TestCloudWebhookOverMQTTMismatchDropped`.
4. **The full body, including the e-mail and `value1..3`, arrives over MQTT (the documented relay forwards it as-is).** It is applied, and no log line contains the personal values. Test: Task 4, `TestCloudWebhookOverMQTTNeverLogsPayload`.
5. **The same event arrives over MQTT and over the bridge, or twice over MQTT** (QoS 1 redelivery). It is published once. Test: Task 4, `TestCloudWebhookOverMQTTDeduplicated`.

## File Structure

| File | Change |
|---|---|
| `internal/hass/*` → `internal/mqtt/*` | client, topics and their tests move; package `mqtt`; paho imported as `paho` |
| `internal/mqtt/hass-discovery/` (package `hassdiscovery`) | discovery payload, discovery topic, HA birth topic, golden file and tests move here |
| `internal/app/app.go`, `CLAUDE.md` | imports, wiring of the discovery implementation, layout and `-update` command |
| `internal/gateway/manager.go` | add `DeliverCloudWebhook(lockID string, body []byte) (cloud.WebhookEvent, error)` (decode + bind + deliver); `DeliverCloudEvent` stays as the typed entry point it calls |
| `internal/gateway/manager_test.go` | tests for `DeliverCloudWebhook` |
| `internal/webhook/handler.go` | cloud route calls `DeliverCloudWebhook`; status mapping by error; `Sink` interface gains `DeliverCloudWebhook` |
| `internal/webhook/handler_test.go` | existing cloud-route tests keep passing; fake sink updated |
| `internal/mqtt/topics.go` | `CloudWebhook(id)`, `CloudWebhookWildcard()` |
| `internal/mqtt/client.go` | `ClientConfig.CloudWebhooks`, `type CloudWebhook`, `CloudWebhooks() <-chan CloudWebhook`, subscription in `onConnect`, `onCloudWebhook` filter |
| `internal/mqtt/client_test.go` | broker tests for the filter rules |
| `internal/config/config.go`, `config_test.go` | `MQTT.CloudWebhooks bool` (`yaml:"cloud_webhooks"`), env `LOQED_MQTT__CLOUD_WEBHOOKS` via the existing mechanism |
| `internal/app/app.go` | `Deps.CloudWebhooks` from either source; `ClientConfig.CloudWebhooks`; `forwardCloudWebhooks` goroutine; per-lock topic log at startup |
| `internal/app/app_test.go` | end-to-end over the in-process broker |
| `addon/config.yaml`, `addon/translations/en.yaml` | option, schema, translation |
| `addon/DOCS.md`, `README.md` | the Nabu Casa / Home Assistant automation recipe, topic, trust note |
| `CLAUDE.md` | one line in the invariants: retained `cloud_webhook` messages are ignored too |

---

### Task 1: Split `internal/hass` into `internal/mqtt` and `internal/mqtt/hass-discovery`

**Files:**
- Move: `internal/hass/client.go`, `client_test.go`, `topics.go` → `internal/mqtt/` (package `mqtt`; the paho import is aliased `paho` to avoid confusion with the package name)
- Move: `internal/hass/discovery.go`, `discovery_test.go`, `testdata/discovery_lock1.golden.json` → `internal/mqtt/hass-discovery/` (package `hassdiscovery`)
- Modify: `internal/app/app.go`, `internal/app/app_test.go`, `CLAUDE.md` (layout table, dependency rule, golden `-update` command)
- Delete: `internal/hass/`

**Interfaces:**
- Consumes: the current `hass` API (`Client`, `ClientConfig`, `Topics`, `TopicID`, `LockInfo`, `DiscoveryPayload`).
- Produces, in `internal/mqtt`:
  - `Client`, `ClientConfig`, `Command`, `LockInfo`, `TopicID`, unchanged apart from the package name;
  - `Topics{Base string}` without `DiscoveryPrefix`, `Discovery()` or `HAStatus()`;
  - `type Discovery interface { BirthTopic() string; Topic(lockID string) string; Payload(l LockInfo) ([]byte, error) }`;
  - `ClientConfig.Discovery Discovery` replaces `ClientConfig.HAEnabled`: nil means Home Assistant is off.
- Produces, in `internal/mqtt/hass-discovery` (package `hassdiscovery`):
  - `func New(prefix string, topics mqtt.Topics, version string) *Discovery`;
  - `*Discovery` implements `mqtt.Discovery`. The discovery topic, the birth topic and the payload are identical to today's.
- `internal/app` builds `hassdiscovery.New(...)` only when `homeassistant.enabled` is true and passes it as `ClientConfig.Discovery`.

**BDD skeleton**

```
hassdiscovery.Discovery
    Topic
        - returns "<prefix>/device/loqed_<topic id>/config" (same as before the split)
    BirthTopic
        - returns "<prefix>/status"
    Payload
        - matches the existing golden file byte for byte (golden file moved, not regenerated)

mqtt.Client
    onConnect
        With a Discovery
            - publishes each lock's discovery payload, retained, to Discovery.Topic(lock id)
            - subscribes to Discovery.BirthTopic(); an "online" message republishes discovery
        With Discovery nil
            - publishes no discovery and subscribes to no Home Assistant topic
    SetLocks
        With a Discovery, on a removed lock
            - clears its retained discovery topic (empty payload) along with its other retained topics
        With Discovery nil, on a removed lock
            - clears its other retained topics; touches no discovery topic

Package boundaries
    - only internal/mqtt imports github.com/eclipse/paho.mqtt.golang
    - internal/mqtt imports nothing from internal/mqtt/hass-discovery
```

- [ ] **Step 1:** Move the files with `git mv` (keeps history), rename the packages and fix imports. Run `go build ./...`. Expected: success.
- [ ] **Step 2:** Write the new tests from the skeleton:
  - the `Discovery`-nil client tests, replacing the `HAEnabled=false` ones;
  - the topic tests in `hassdiscovery`;
  - a boundary test that fails if a package other than `internal/mqtt` imports paho, or if `internal/mqtt` imports `hass-discovery`. It uses `go list -deps -json` or `go/build`.
- [ ] **Step 3:** Run `go test ./internal/mqtt/... ./internal/app`. Expected: FAIL on the new tests only.
- [ ] **Step 4:** Introduce the `mqtt.Discovery` interface and implement it in `hassdiscovery`. Move the discovery-topic and birth-topic knowledge out of `Topics`, and wire it in `internal/app`.
- [ ] **Step 5:** Run `gofmt -l .`, `go vet ./...`, `go test -race ./...`, `go test -race -count=8 ./internal/mqtt/...` and golangci-lint. Expected: all clean. The golden file has no diff.
- [ ] **Step 6:** Update `CLAUDE.md`:
  - the layout table;
  - the dependency rule (`internal/mqtt` is the only importer of the MQTT library; `hass-discovery` holds everything HA specific);
  - the `-update` command, which becomes `go test ./internal/mqtt/hass-discovery -run TestDiscoveryPayload -update`.
- [ ] **Step 7:** Commit with the message `mqtt: split MQTT client from Home Assistant discovery`.

### Task 2: Gateway — one decode-and-route path for cloud webhooks

**Files:**
- Modify: `internal/gateway/manager.go`, `internal/webhook/handler.go`
- Test: `internal/gateway/manager_test.go`, `internal/webhook/handler_test.go`

**Interfaces:**
- Consumes: `cloud.ParseWebhook(body []byte) (cloud.WebhookEvent, error)`, `(*Supervisor).BindCloudID(id string) error`, `Manager.DeliverCloudEvent(lockID string, ev cloud.WebhookEvent) error`.
- Produces: `func (m *Manager) DeliverCloudWebhook(lockID string, body []byte) (cloud.WebhookEvent, error)`.
  - Returns the decoded event so callers can log `ev.LockID` on a mismatch; it is the zero value when decoding failed.
  - Errors are `loqed.ErrInvalidPayload`, `ErrUnknownLock`, `ErrCloudIDMismatch` or `ErrBusy`, wrapped and matchable with `errors.Is`.
  - `webhook.Sink` gains the same method.

**BDD skeleton**

```
Manager
    DeliverCloudWebhook
        On a valid body for a known lock
            - the supervisor receives one CloudEventMsg with the decoded event
            - the body's lock_id is stored as the lock's cloud_webhook_id on first use
            - returns the decoded event and nil
        On a body whose lock_id matches the stored cloud_webhook_id
            - delivers the event; returns nil
        On a body whose lock_id differs from the stored cloud_webhook_id
            - returns an error matching ErrCloudIDMismatch
            - the returned event carries the body's lock_id
            - the supervisor receives nothing
        On an undecodable body (not JSON, no lock_id, nothing to report)
            - returns an error matching loqed.ErrInvalidPayload
            - the supervisor receives nothing; no cloud id is bound
        On an unknown lock id
            - returns ErrUnknownLock; the body is not bound anywhere
        On a full supervisor queue
            - returns ErrBusy
        On a documented-shape body (alphanumeric lock_id)
            - is delivered and binds that id like a numeric one

Webhook handler
    POST /cloud/<secret>/<lock-id>
        On every existing case (wrong secret, unknown lock, invalid body, mismatch, busy, success)
            - responds with the same status code as before the refactor (404, 404, 400, 409, 503, 200)
            - logs the same lines as before; never the body
```

- [ ] **Step 1:** Write the `DeliverCloudWebhook` tests in `manager_test.go`, one per effect, using the existing manager harness and the real-shape payloads (numeric `lock_id`).
- [ ] **Step 2:** Run `go test ./internal/gateway -run TestDeliverCloudWebhook`. Expected: FAIL (undefined method).
- [ ] **Step 3:** Implement `DeliverCloudWebhook` in `manager.go`. It decodes with `cloud.ParseWebhook` and delegates to `DeliverCloudEvent`.
- [ ] **Step 4:** Point the HTTP cloud route at `DeliverCloudWebhook`. It maps errors to status codes with `errors.Is`, adding `ErrInvalidPayload` → 400 to the existing mapping. Update the fake sink in `handler_test.go`.
- [ ] **Step 5:** Run `go test -race ./internal/gateway ./internal/webhook`. Expected: PASS, with the existing handler tests unchanged.
- [ ] **Step 6:** Commit with the message `gateway: one decode-and-route path for cloud webhooks`.

### Task 3: MQTT — `cloud_webhook` topic input

**Files:**
- Modify: `internal/mqtt/topics.go`, `internal/mqtt/client.go`
- Test: `internal/mqtt/client_test.go`

**Interfaces:**
- Consumes: the existing `Client` lock map (topic id → `LockInfo`), `waitSubscribe` and `onConnect`.
- Produces the following, all in `internal/mqtt`:
  - `ClientConfig.CloudWebhooks bool`
  - `type CloudWebhook struct { LockID string; Body []byte }`, where `LockID` is the real lock id, not the topic id
  - `func (c *Client) CloudWebhooks() <-chan CloudWebhook`, a buffered channel the same size as the command channel
  - `Topics.CloudWebhook(id string) string`
  - `Topics.CloudWebhookWildcard() string`

**BDD skeleton**

```
Topics
    CloudWebhook
        - returns "<base>/<id>/cloud_webhook"
    CloudWebhookWildcard
        - returns "<base>/+/cloud_webhook"

Client
    onConnect
        With CloudWebhooks enabled
            - subscribes to the wildcard at QoS 1 on the first connect and after every reconnect
        With CloudWebhooks disabled
            - does not subscribe; a message published to the topic never appears on CloudWebhooks()
    onCloudWebhook
        On a non-retained message for a known lock
            - emits CloudWebhook{LockID: real lock id, Body: payload bytes unchanged}
        On a retained message (initial connect or reconnect)
            - emits nothing
            - logs a warning naming the topic and telling the user to publish without retain
        On a message for an unknown topic id
            - emits nothing; logs a warning naming the topic
        On a payload larger than 64 KiB
            - emits nothing; logs a warning with the size, never the payload
        On a full channel
            - drops the message; logs a warning with the lock id
        Always
            - never logs the payload
```

- [ ] **Step 1:** Write the topic tests, then the broker-backed client tests with `internal/testutil`: one per scenario above, including `TestCloudWebhookRetainedIgnored` (initial connect and reconnect) and `TestCloudWebhookNotSubscribedWhenDisabled`. Capture logs with a `slog` text handler on a buffer, and assert that a marker string from the payload never appears.
- [ ] **Step 2:** Run `go test ./internal/mqtt -run 'CloudWebhook'`. Expected: FAIL.
- [ ] **Step 3:** Implement the topics, config field, channel, subscription and filter. The filter mirrors `onCommand` (retained check first, then lock lookup) and adds the size check.
- [ ] **Step 4:** Run `go test -race ./internal/mqtt/...`, then `-count=8`, because broker tests had a one-off hang before. Expected: PASS. The discovery golden file is unchanged.
- [ ] **Step 5:** Commit with the message `mqtt: accept cloud webhook bodies on <id>/cloud_webhook`.

### Task 4: Config, wiring, end to end

**Files:**
- Modify: `internal/config/config.go`, `internal/app/app.go`, `addon/config.yaml`, `addon/translations/en.yaml`
- Test: `internal/config/config_test.go`, `internal/app/app_test.go`

**Interfaces:**
- Consumes: `mqtt.Client.CloudWebhooks()`, `Manager.DeliverCloudWebhook`, `mqtt.Topics.CloudWebhook`.
- Produces:
  - `config.MQTT.CloudWebhooks bool`;
  - `forwardCloudWebhooks(ctx, mq, manager, log)` in `internal/app`, the sibling of `forwardCommands`.

**BDD skeleton**

```
Config
    Load
        With mqtt.cloud_webhooks absent
            - CloudWebhooks is false
        With mqtt.cloud_webhooks: true in YAML, options.json or LOQED_MQTT__CLOUD_WEBHOOKS=true
            - CloudWebhooks is true (env overrides YAML overrides options.json)

App
    Run
        With mqtt.cloud_webhooks true and no public_url
            - the gateway runs with "cloud webhooks configured" (cloud mode uses push freshness)
            - logs each lock's cloud_webhook topic once at info
            - the HTTP /cloud/ route stays unrouted (no cloud secret is generated)
        With neither option
            - cloud webhooks are not configured (unchanged behaviour)
    forwardCloudWebhooks
        On a valid real-shape body published to <base>/<id>/cloud_webhook
            - the lock's state and event topics reflect the event, with source null for a keyless event
        On the same event published twice within the dedup window
            - one event is published
        On the same event arriving over the bridge and over MQTT
            - one event is published
        On a body whose lock_id differs from the bound one
            - nothing is published; a warning names lock_id and cloud_lock_id
        On an undecodable body
            - nothing is published; a warning names the lock
        On a full body including key_account_email and value1..3
            - the event is applied
            - no log line contains the e-mail, the account name or value2/value3
        On context cancel
            - the goroutine returns
```

- [ ] **Step 1:** Write the config tests (each source plus the default).
- [ ] **Step 2:** Write the end-to-end tests in `app_test.go` on the existing in-process broker and fake bridge/cloud setup:
  - `TestCloudWebhookOverMQTTApplied`
  - `TestCloudWebhookOverMQTTDeduplicated`
  - `TestCloudWebhookOverMQTTMismatchDropped`
  - `TestCloudWebhookOverMQTTNeverLogsPayload`
  - a test for push freshness without `public_url`
- [ ] **Step 3:** Run `go test ./internal/config ./internal/app -run 'CloudWebhook'`. Expected: FAIL.
- [ ] **Step 4:**
  - Implement the config field.
  - Set `Deps.CloudWebhooks` from either source and pass `ClientConfig.CloudWebhooks`.
  - Add the forwarder goroutine. It logs every rejection at warn by error class, with `lock_id` plus `cloud_lock_id` on a mismatch.
  - Add the startup topic log line.
  - Add `cloud_webhooks: bool?` to the add-on `mqtt` schema, with its translation.
- [ ] **Step 5:** Run the full suite: `gofmt -l .`, `go vet ./...`, `go test -race ./...` and golangci-lint. Expected: all clean.
- [ ] **Step 6:** Commit with the message `app: wire cloud webhooks over MQTT`.

### Task 5: Documentation — Home Assistant / Nabu Casa recipe

BDD skeleton skipped: documentation only.

**Files:**
- Modify: `addon/DOCS.md`, `README.md`, `CLAUDE.md`

**Required content** (prose and YAML for the user, following the spec's 6.1 and 9):

- **When to use it:** you want cloud webhooks, but there's no reverse proxy, or Home Assistant Cloud (Nabu Casa) is already set up. Set `mqtt.cloud_webhooks: true`.
- **One automation:**
  - one webhook trigger per lock, with `local_only: false` and a trigger `id` equal to the lock's topic id from the startup log;
  - `allowed_methods: [POST]`;
  - the trigger's Nabu Casa URL (copied from the trigger's UI) is registered for that lock in the API section of app.loqed.com.
- **The action:** `mqtt.publish` to `loqed/{{ trigger.id }}/cloud_webhook` with `retain: false` and `qos: 1`. The payload is the trigger's JSON body re-serialized unchanged (`trigger.json` through `to_json`); no field filtering.
- **Why `retain: false` matters:** retained messages are ignored.
- **Trust:**
  - the Nabu Casa URL token is the secret, so treat it like a password;
  - broker users who can publish to `cloud_webhook` can inject events, so restrict them with ACLs as for `command`;
  - cloud webhooks are unsigned (accepted risk).
- **Mapping is per lock:** don't point one lock's URL at another lock's topic. A mismatch is logged and dropped.
- **`CLAUDE.md` invariant line:** retained messages on `<base>/<id>/command` **and** `<base>/<id>/cloud_webhook` are ignored.

- [ ] **Step 1:** Write the README and DOCS sections, and update `CLAUDE.md`.
- [ ] **Step 2:** Run `go test ./...` (the docs don't affect it; this confirms nothing else changed) and `gofmt -l .`.
- [ ] **Step 3:** Commit with the message `docs: cloud webhooks via Home Assistant and MQTT`.

### Task 6: V10 — Home Assistant relay against the real lock (manual, user-assisted)

BDD skeleton skipped: a manual verification on real hardware. The steps and the expected outcomes are its contract.

- [ ] **Step 1:** With the user, create the automation from Task 5 in their Home Assistant. Check in Developer Tools → Template that the payload template turns a synthetic sample body (fake values) back into identical JSON, numbers kept as numbers.
- [ ] **Step 2:**
  - Run the gateway (scratch build) with `mqtt.cloud_webhooks: true` against the user's broker.
  - Subscribe to `loqed/+/cloud_webhook` and record only the key names and value types of each payload, never the values.
- [ ] **Step 3:** The user registers the Nabu Casa URL for the lock at app.loqed.com and operates the lock by hand. This test sends no commands, so no lock actuation needs authorization.
  - Expected: every payload has the same keys as LOQED's body (2.2), with numbers still numbers and the `lock_id` numeric.
  - Expected: the gateway publishes each event once, and every bridge/cloud pair is matched.
  - Measure the Nabu Casa relay's delay against the bridge copy.
- [ ] **Step 4:** Record the outcome as V10 in spec 2.5, correct the recipe in `addon/DOCS.md` if the check found a difference, and commit with the message `docs: record V10 (Home Assistant relay)`.
- [ ] **Step 5:** Clean up with the user: remove the test automation or keep it as their production relay, and remove any scratch containers.

## Spec coverage

| Spec item | Task |
|---|---|
| 6.1 `cloud_webhook` topic, retained/unknown/size rules | 3 |
| 6.1 decode and route like HTTP, warn on mismatch/invalid/busy, payload never logged | 2, 4 |
| 7 shared decode-and-route, HTTP status codes unchanged | 2 |
| 5.1 `mqtt.cloud_webhooks`; add-on schema | 4 |
| 5.5 "cloud webhooks configured" from either source | 4 |
| 5.4 step 6 topic log | 4 |
| 5.7 dedup across MQTT, bridge and redelivery | 4 (existing dedup, tested end to end) |
| 9 DOCS recipe, trust note | 5 |
| 3 package layout (`internal/mqtt`, `internal/mqtt/hass-discovery`) | 1 |
| 10 fixtures in real shapes | done in `31d7e70` |
| 10 mqtt retained/disabled tests | 3 |
| V10 | 6 |

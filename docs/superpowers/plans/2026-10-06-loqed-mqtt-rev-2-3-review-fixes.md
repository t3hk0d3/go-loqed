# loqed-mqtt rev 2.3: Pre-merge Review Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the critical and important findings of the pre-merge review of `feat/v1` (C1, I1–I4) and the CI branch name, as specified in spec rev 2.3.

**Architecture:**
- **C1** is a startup ordering change in `internal/app`, plus one new `store` method. The cache must be writable before any cloud or portal call, and the cloud webhook secret is resolved before the first cloud call.
- **I1, I2, I3 and I4** live in `internal/gateway`, on the supervisor goroutine:
  - I1 caps the local attempt timeout in the command pipeline.
  - I4 moves the failover a command triggers into the pipeline's existing "after the command resolves" hook (`resolved`, which already runs `refreshAfter`).
  - I3 widens the event feed's cross-feed pairing to 5 min without widening same-feed deduplication.
  - I2 adds a "webhook delivery confirmed" flag that drives a 1-minute `/status` loop and two detectors.

**Tech Stack:** Go 1.27, `log/slog`, stdlib `testing`. Gateway tests use the fake bridge, cloud and clock in `internal/gateway/harness_test.go`. App tests use `httptest` fakes and the in-process broker (`internal/testutil`).

**Spec:** `docs/superpowers/specs/2026-10-04-loqed-mqtt-gateway-design.md`, rev 2.3 (`488bc4c`). The relevant sections:
- the rev 2.3 header;
- 5.3 (last sentences);
- 5.4 step 2;
- 5.5: "Entering `local`", the "Webhook delivery is confirmed, not assumed" bullet, the `GET /status` bullet and the failover bullet;
- 5.7, deduplication;
- 5.8: Order and the CommandPipeline BDD;
- 8, "Fatal at startup";
- 9, DOCS;
- 10, the gateway and app items.

## Styleguide

The project styleguide is `CLAUDE.md` (Conventions, Safety invariants, Secrets) together with `.golangci.yml` (golangci-lint v2.14.0, gofmt). Every task follows it. The rules that matter here:

- `internal/gateway` alone knows lock modes, timers and deduplication. `internal/app` only wires packages and decides what is fatal at startup. `internal/store` owns the cache file.
- Supervisor state is touched only on the supervisor goroutine. A new field joins the `Supervisor` struct with a short comment, next to its group (timers, confirmation, events).
- New durations go into `gateway.Timing` with a comment and a value in `DefaultTiming`. No literals in logic.
- Errors:
  - wrap with `%w` around existing sentinels (`store.ErrWrite`, `loqed.ErrUnreachable`);
  - callers branch with `errors.Is`;
  - storage errors name the path.
- Log lines:
  - warnings that can repeat go through `s.warn` (rate-limited per message, 10 min);
  - never log tokens, keys, signed commands or webhook bodies;
  - the webhook URL (`http://<host>:<port>/webhook/<lock-id>`) is not secret and may be logged.
- Tests:
  - named `Test<Unit><Scenario>`;
  - gateway tests drive `harness` with the fake clock (`h.now`) and real `Timing` defaults unless the test says otherwise;
  - no real sleeps in gateway tests.
- Commits: `<area>: <what>`, ending with the Co-Authored-By and Claude-Session trailers.
- Doc comments explain why, following the density of the surrounding code.

## Global Constraints

- Startup order (5.4 step 2):
  1. load the cache;
  2. check that `cache_path` is writable (fatal, a storage error naming the path);
  3. resolve the cloud webhook secret if `webhook.public_url` is set;
  4. only then resolve the token, mint, or refresh.
  - There is no cloud or portal request before the check.
  - At runtime a write failure is logged, never fatal.
- Timing defaults:
  - `RequestTimeout` 5 s;
  - `LocalCutoff` 10 s and `OpenLocalCutoff` 3 s before deadlines of 30 s and 10 s;
  - `Liveness` = `liveness_interval` (default 1 min);
  - `Reconcile` = `reconcile_interval` (default 24 h);
  - `WebhookConfirm` 30 s;
  - `WebhookRetry` 10 min;
  - `DuplicateWindow` 10 s (0 = deduplication off);
  - new: `PairingWindow` 5 min.
- Local attempt timeout = `min(RequestTimeout, cutoff − now)`. A local attempt never starts at or after the cutoff (unchanged).
- Webhook delivery:
  - It is unconfirmed on entering `local`, after every webhook **creation** on the bridge, and while registration fails.
  - It is confirmed by any signed bridge webhook, including deduplicated copies.
  - While unconfirmed, `/status` is read every `Liveness`, with the hint rules applied.
  - It returns to unconfirmed, with a rate-limited warning naming the webhook address `host:port`, when either:
    - (a) a `/status` read shows a known bolt state different from the current known one, with no bridge webhook within `Reconcile`; or
    - (b) a command delivered via the bridge (`sent`, via `local`) reaches `sentAt + WebhookConfirm` with no bridge webhook since `sentAt`.
- Pairing:
  - Within `DuplicateWindow` the deduplication rules are unchanged.
  - For up to `PairingWindow`, an unpaired published event absorbs the other feed's first copy.
  - It never drops a same-feed repeat after `DuplicateWindow`.
  - With `DuplicateWindow` 0, nothing is ever dropped.
- Failover from a command's local failures (the third consecutive HTTP failure) runs in `resolved`, after the command's cloud attempt. The other `httpFailure` callers (reconcile, registration, liveness) keep failing over at once.
- CI runs on pushes to `master` and on pull requests.

## Review Focus

1. **A bridge that accepts TCP and then hangs during `OPEN`.** Every local attempt ends by the 7 s cutoff and the one cloud attempt still starts before the 10 s deadline. Test: Task 3, `TestOpenWithHangingBridgeStillSendsViaCloud`.
2. **A real change back with a lost copy inside the pairing window.** For example, cloud `day` arrives but its bridge copy is lost, then `night` arrives on both feeds, then a real `day` reaches the bridge 2 min later. The second `day` must be published, so the pairing window must not turn into a same-type filter. Test: Task 5, `TestPairingWindowNeverDropsARealChangeBack`.
3. **Bridge webhooks blocked by a firewall for days.** One warning per 10 min (rate limit), not one per minute; `/status` every minute, never more often; no cloud calls. Test: Task 6, `TestUnconfirmedWarningIsRateLimited` and `TestUnconfirmedReadsUseNoCloudBudget`.
4. **A corrupt or other-version cache in a writable directory.** It still starts and rebuilds from the cloud; the writability check must not treat "unreadable" as "unwritable". Test: Task 2, `TestCorruptCacheInWritableDirectoryStillStarts`.
5. **A command whose local failures trigger failover while a second command is queued.** The first goes via the cloud, then failover runs. The queued command runs in cloud mode and never retries the bridge. Test: Task 4, `TestQueuedCommandAfterCommandFailoverUsesCloud`.

## File Structure

| File | Change |
|---|---|
| `.github/workflows/ci.yml` | `push.branches: [master]` |
| `internal/store/store.go`, `store_test.go` | `func (s *Store) CheckWritable() error`: rewrites the current cache atomically; the error wraps `ErrWrite` and names the path |
| `internal/app/app.go` | startup: `CheckWritable` and `InstallID` failures are fatal; `ensureCloudSecret` moves before `initialRefresh` |
| `internal/app/app_test.go` | unwritable-cache, corrupt-cache and secret-ordering startup tests |
| `internal/gateway/pipeline.go` | local attempt timeout capped by the cutoff; `command.failoverAfter`; `resolved` runs the deferred failover |
| `internal/gateway/local.go` | `httpFailure` split so a caller can defer the failover; webhook-delivery confirmation (flag, 1-minute reads, detectors, warning); `ensureWebhook` reports creation |
| `internal/gateway/eventfeed.go` | pairing window and the in-order rule (see Task 5) |
| `internal/gateway/supervisor.go` | `Timing.PairingWindow`; new supervisor fields |
| `internal/gateway/harness_test.go` | `fakeBridge` records each command's context budget and can block until its context ends |
| `internal/gateway/pipeline_test.go`, `local_test.go`, `eventfeed_test.go`, `failover_test.go` | tests from the BDD skeletons |
| `addon/DOCS.md` | step 4 of setup: what happens without the firewall rule |
| `docs/superpowers/specs/…design.md` | 5.7: one clarifying sentence (Task 5) |

---

### Task 1: CI runs on `master`

**Files:**
- Modify: `.github/workflows/ci.yml:4`

**Interfaces:** none.

No BDD skeleton: this is a one-line workflow configuration change.

- [ ] **Step 1:** Change `branches: [main]` to `branches: [master]`.
- [ ] **Step 2:** Run `python3 -c 'import yaml,sys; print(yaml.safe_load(open(".github/workflows/ci.yml"))[True]["push"])'`. Expected: `{'branches': ['master']}`. YAML 1.1 parses the `on` key as `True`.
- [ ] **Step 3:** Commit with the message `ci: run on pushes to master`.

### Task 2 (C1): An unwritable cache stops the gateway at startup

**Files:**
- Modify: `internal/store/store.go`, `internal/app/app.go` (lines ~65–125)
- Test: `internal/store/store_test.go`, `internal/app/app_test.go`

**Interfaces:**
- Consumes: `store.Open`, `(*Store).InstallID`, `store.ErrWrite`, `ensureCloudSecret(cfg, st)`, `initialRefresh(...)`.
- Produces: `func (s *Store) CheckWritable() error`.
  - It writes the in-memory cache to `path`, with the same atomic write and `0600` mode as `Update`.
  - On failure it returns an error matching `ErrWrite` whose text contains the path.
  - The in-memory cache is not changed.

**BDD skeleton**

```
Store
    CheckWritable
        On a writable directory with a loaded cache
            - returns nil
            - the file on disk still decodes to the same cache (no field lost)
        On a writable directory without a cache file
            - returns nil; the file now exists with mode 0600
        On a read-only directory (existing cache or none)
            - returns an error matching ErrWrite
            - the error text contains the cache path
            - Snapshot() is unchanged

app.Run (startup)
    With an unwritable cache_path (no cache yet)
        - returns an error matching store.ErrWrite that names the path
        - the fake cloud and the fake portal received no request
    With an unwritable cache_path holding a valid cache with install_id
        - returns an error matching store.ErrWrite that names the path
        - the fake cloud received no request
    With a corrupt cache in a writable directory
        - starts (Ready is called) and rebuilds the cache from the cloud
    With webhook.public_url set and no secret configured or cached
        - the cache file already holds cloud_secret when the first cloud request arrives
```

The unwritable tests chmod a `t.TempDir()` to `0o500` and call `t.Skip` when `os.Geteuid() == 0`, because root ignores directory permissions.

- [ ] **Step 1:** Write the `Store.CheckWritable` tests in `internal/store/store_test.go`:
  - `TestCheckWritableKeepsLoadedCache`;
  - `TestCheckWritableCreatesMissingFile`;
  - `TestCheckWritableReadOnlyDirectory`.
- [ ] **Step 2:** Run `go test ./internal/store -run CheckWritable`. Expected: build failure, `CheckWritable` undefined.
- [ ] **Step 3:** Implement `CheckWritable`, sharing the write path with `Update`.
- [ ] **Step 4:** Run `go test -race ./internal/store`. Expected: PASS.
- [ ] **Step 5:** Write the app tests in `internal/app/app_test.go`:
  - `TestUnwritableCacheFailsBeforeAnyCloudCall`: email and password configured, so a portal mint would be attempted; both fake servers count requests.
  - `TestUnwritableExistingCacheFailsBeforeAnyCloudCall`.
  - `TestCorruptCacheInWritableDirectoryStillStarts`.
  - `TestCloudSecretIsSavedBeforeFirstCloudCall`: the fake cloud handler reads the cache file on its first request.
- [ ] **Step 6:** Run `go test ./internal/app -run 'Unwritable|CorruptCache|CloudSecretIsSaved'`. Expected:
  - the unwritable tests FAIL (Run proceeds and calls the cloud);
  - the secret-ordering test FAILS (no secret yet at the first request);
  - the corrupt-cache test PASSES (it guards the change).
- [ ] **Step 7:** Change `Run`:
  - after `store.Open` and the status warnings, call `st.CheckWritable()`, then `st.InstallID()`;
  - either error returns `fmt.Errorf("the credential cache cannot be written; fix the permissions of its directory: %w", err)`;
  - move the `public_url` / `ensureCloudSecret` block before `initialRefresh`;
  - remove the "running from memory" log line for `InstallID`.
- [ ] **Step 8:** Run `go test -race ./internal/app ./internal/store`. Expected: PASS.
- [ ] **Step 9:** Commit with the message `app: refuse to start with an unwritable credential cache`.

### Task 3 (I1): A local attempt never runs past the local cutoff

**Files:**
- Modify: `internal/gateway/pipeline.go` (`attemptLocal`, ~212–220)
- Test: `internal/gateway/pipeline_test.go`, `internal/gateway/harness_test.go`

**Interfaces:**
- Consumes: `command.cutoff`, `command.deadline`, `Timing.RequestTimeout`.
- Produces: no API change. `fakeBridge` gains two fields:
  - `cmdBudgets []time.Duration`: the time left on each `Command` context, like `fakeCloud.cmdBudgets`;
  - `hang func(ctx context.Context) error`: an optional hook run inside `Command`.

**BDD skeleton**

```
CommandPipeline
    attemptLocal
        With more than RequestTimeout left before the cutoff
            - the bridge call's context has RequestTimeout (5 s) left
        With less than RequestTimeout left before the cutoff (OPEN, 2 s before its 7 s cutoff)
            - the bridge call's context ends at the cutoff (≤ 2 s left)
        On a bridge that hangs until its context ends (OPEN)
            - every local attempt ends by the cutoff (7 s after arrival)
            - the one cloud attempt starts before the 10 s deadline
            - the command ends sent via cloud, not expired
```

The hang hook returns an error matching `loqed.ErrUnreachable` (a TCP connect timeout). It advances `h.now` by the context's remaining budget, so the fake clock reflects the time the hang took.

- [ ] **Step 1:** Add `cmdBudgets` and `hang` to `fakeBridge`.
- [ ] **Step 2:** Write the tests:
  - `TestLocalAttemptTimeoutIsRequestTimeout`;
  - `TestLocalAttemptTimeoutEndsAtCutoff`;
  - `TestOpenWithHangingBridgeStillSendsViaCloud`.
- [ ] **Step 3:** Run `go test ./internal/gateway -run 'LocalAttemptTimeout|HangingBridge'`. Expected:
  - `EndsAtCutoff` FAILS (the budget is up to the deadline);
  - `HangingBridge` FAILS (expired);
  - `IsRequestTimeout` PASSES.
- [ ] **Step 4:** Cap the timeout at `a.cutoff.Sub(now)` instead of `a.deadline.Sub(now)`.
- [ ] **Step 5:** Run `go test -race ./internal/gateway`. Expected: PASS.
- [ ] **Step 6:** Commit with the message `gateway: end local command attempts at the local cutoff`.

### Task 4 (I4): Failover triggered by a command runs after the command

**Files:**
- Modify: `internal/gateway/pipeline.go` (`command`, the `attemptLocal` ErrUnreachable branch, `resolved`), `internal/gateway/local.go` (`httpFailure`)
- Test: `internal/gateway/failover_test.go` (or `pipeline_test.go`, next to `TestUnreachableBridgeIsRetriedWithBackoffThenCloud`)

**Interfaces:**
- Consumes: `(*Supervisor).localUnreachable(ctx, err)`, `commandPipeline.resolved`.
- Produces (internal to `gateway`):
  - `func (s *Supervisor) countHTTPFailure(err error) bool` counts one failure and reports whether the threshold is reached, without acting on it.
    - `httpFailure(ctx, err)` keeps its behaviour (unauthorized → refresh; threshold → `localUnreachable`), built on `countHTTPFailure`.
  - `command.failoverAfter bool`: run `localUnreachable` once the command resolves.

**BDD skeleton**

```
CommandPipeline
    attemptLocal
        On ErrUnreachable past the cutoff, when this is the third consecutive HTTP failure
            - the cloud command is sent before any credential refresh
            - the lock is still in local mode while the cloud command is sent
            - no refresh and no mode change happen before the command resolves
        On ErrUnreachable past the cutoff, below the failure threshold
            - sends via the cloud; no refresh; mode unchanged (as today)
    resolved
        With failoverAfter set and the lock still in local mode
            - runs one credential refresh (reason unreachable)
            - same IP after the refresh: enters cloud mode
            - new IP after the refresh: retries local mode
        With failoverAfter set but the lock already left local mode
            - does nothing more (no second refresh)
    Submit (queued behind a command that triggered failover)
        - the queued command runs after failover, in cloud mode, with no bridge call
```

To observe the ordering, the test's `Refresh` dependency records how many cloud commands `h.cloud` had received when it ran. Use the harness's `tweak` parameter for this.

- [ ] **Step 1:** Write the tests:
  - `TestCommandFailoverRunsAfterTheCloudAttempt`;
  - `TestCommandBelowThresholdDoesNotFailOver`;
  - `TestCommandFailoverSkippedWhenAlreadyLeftLocal`;
  - `TestQueuedCommandAfterCommandFailoverUsesCloud`.
- [ ] **Step 2:** Run `go test ./internal/gateway -run 'CommandFailover|BelowThreshold|AfterCommandFailover'`. Expected: `RunsAfterTheCloudAttempt` FAILS (the refresh ran before the cloud command, mode already `cloud`). Record which others fail.
- [ ] **Step 3:** Add `countHTTPFailure` and rebuild `httpFailure` on it. In the ErrUnreachable branch:
  - count the failure;
  - set `a.failoverAfter` when the threshold is reached.
  
  In `resolved`, run `localUnreachable(ctx, a.lastLocalErr)` when `failoverAfter` is set and the mode is still `local`, after the existing `refreshAfter` block.
- [ ] **Step 4:** Run `go test -race ./internal/gateway`. Expected: PASS. This includes the existing `TestUnreachableBridgeIsRetriedWithBackoffThenCloud` and the failover tests.
- [ ] **Step 5:** Commit with the message `gateway: fail over after the command that triggered it`.

### Task 5 (I3): A late copy from the other feed cannot roll the state back

**Files:**
- Modify: `internal/gateway/eventfeed.go` (`copyOfPublished`, `lockEvent`), `internal/gateway/supervisor.go` (`Timing.PairingWindow`, `DefaultTiming`), spec 5.7 (one sentence)
- Test: `internal/gateway/eventfeed_test.go`

**Interfaces:**
- Consumes: `(*Supervisor).isDuplicate(feed, eventType, key, now)` and its callers in `onBridgeEvent` and `onCloudEvent` (unchanged).
- Produces: `Timing.PairingWindow time.Duration`, which defaults to `5 * time.Minute`. A non-positive value means no pairing beyond `DuplicateWindow`.

**The in-order rule.** This design detail is not in the spec. Without it, a 5 min pairing window turns a lost copy into a filter that drops a real change back (Review Focus 2). Each feed delivers its events in order. So once feed B has delivered an event that matches, or follows, a published event P, B will never deliver a copy of an older unpaired event from feed A. Its copy was lost, and the older event stops pairing. Inside `DuplicateWindow` the behaviour is unchanged, because the rule only closes entries that could not pair anyway. Task 5 adds one sentence to spec 5.7 saying so.

Pseudocode for `copyOfPublished(feed, type, key, now)`:

```
if DuplicateWindow <= 0: return false
drop entries older than max(DuplicateWindow, PairingWindow)
last := newest entry
if last exists and last.at within DuplicateWindow and last matches (type, key):
    if last.feed != feed: last.paired = true
    return true                                   # rule (a), unchanged
for e in entries, oldest first:
    if e.feed != feed and not e.paired and e matches (type, key)
       and (e.at within DuplicateWindow or e.at within PairingWindow):
        e.paired = true
        close every older unpaired entry of e.feed  # the in-order rule
        return true                               # rule (b), widened
append new entry {feed, type, key, at: now}
close every older unpaired entry of the other feed  # the in-order rule
return false
```

"Close" means the entry can no longer pair. Reusing `paired = true` is fine, because a closed entry only ever matters for pairing.

**BDD skeleton**

```
Supervisor.isDuplicate (event feed)
    Within DuplicateWindow
        - every existing deduplication test passes unchanged
    A copy from the other feed arriving 2 min after the published event (no events in between)
        - is dropped (state and events unchanged)
        - a third copy of it, from either feed, after DuplicateWindow is published (pairing is one copy only)
    A repeat from the same feed after DuplicateWindow
        - is published
    A copy from the other feed arriving after PairingWindow (6 min)
        - is published
    Interleaved late feed: cloud day, cloud night, bridge day (+2 min), bridge night (+2 min)
        - both bridge copies are dropped
    Real change back with a lost copy: cloud day, both night (cloud then bridge), bridge day (+2 min)
        - the second day is published (the bridge's night closed the cloud day)
    Only one feed in use (no cloud webhooks)
        - day, night, day spaced over 3 min are all published
    With DuplicateWindow 0 (event_dedup_enabled: false)
        - nothing is dropped, however close the copies
```

- [ ] **Step 1:** Write the tests:
  - `TestLateCopyFromOtherFeedIsDropped`;
  - `TestPairingAbsorbsOnlyOneCopy`;
  - `TestSameFeedRepeatAfterWindowIsPublished`;
  - `TestCopyAfterPairingWindowIsPublished`;
  - `TestInterleavedLateFeedIsDropped`;
  - `TestPairingWindowNeverDropsARealChangeBack`;
  - `TestSingleFeedChangesBackArePublished`;
  - `TestDedupDisabledDropsNothing`. Keep this one if an equivalent exists; otherwise add it.
- [ ] **Step 2:** Run `go test ./internal/gateway -run 'Copy|Pairing|SameFeed|Interleaved|SingleFeed|DedupDisabled'`. Expected:
  - `LateCopyFromOtherFeed` and `InterleavedLateFeed` FAIL;
  - the rest PASS. They guard the change; `RealChangeBack` especially must keep passing after it.
- [ ] **Step 3:** Add `PairingWindow` to `Timing` and `DefaultTiming`, and implement the pseudocode above in `copyOfPublished`. Update the doc comment of `isDuplicate`.
- [ ] **Step 4:** Run `go test -race ./internal/gateway`. Expected: PASS, including every existing `eventfeed_test.go` case.
- [ ] **Step 5:** Spec 5.7: after the pairing-window sentence, add: "Each feed delivers in order, so once the other feed has delivered a later event, an older event still waiting for its copy stops pairing (that copy was lost); a real change back is therefore never absorbed."
- [ ] **Step 6:** Commit with the message `gateway: pair cross-feed copies for 5 min`.

### Task 6 (I2): Bridge webhook delivery is confirmed, not assumed

**Files:**
- Modify:
  - `internal/gateway/local.go`: `tryEnterLocal`, `registerWebhook`, `ensureWebhook`, `tickLocal`, `reconcile` or `applyStatus` call site, `onBridgeEvent`;
  - `internal/gateway/pipeline.go`: `sent`;
  - `internal/gateway/supervisor.go`: fields;
  - `addon/DOCS.md`: setup step 4.
- Test: `internal/gateway/local_test.go`

**Interfaces:**
- Consumes: `Deps.WebhookURL(rec) (string, error)`, `(*Supervisor).warn`, `reconcile`, and `Timing.Liveness`, `Timing.Reconcile` and `Timing.WebhookConfirm`.
- Produces (internal to `gateway`):
  - `func (s *Supervisor) ensureWebhook(ctx) (created bool, err error)`, which reports whether it created our webhook;
  - new supervisor fields:
    - `webhookConfirmed bool`;
    - `lastBridgeEventAt time.Time`;
    - `nextUnconfirmedRead time.Time`;
    - `bridgeCheckAt time.Time`;
    - `bridgeCheckSince time.Time`;
  - `func (s *Supervisor) unconfirmWebhooks(now time.Time, why string)`: sets the flag, schedules the next read, and warns via `s.warn`. The warning message is fixed, so the rate limit holds, and its attributes are `address` (`host:port` of the webhook URL) and `reason` (`why`).

**BDD skeleton**

```
Supervisor (local mode, bridge webhook delivery)
    tryEnterLocal
        - webhook delivery is unconfirmed after entering local
    registerWebhook
        When our webhook had to be created
            - delivery becomes unconfirmed (even if it was confirmed)
        When our webhook was already registered
            - the confirmation is unchanged
        When registration fails
            - delivery stays unconfirmed; the warning says /status is read every minute until it works
    onBridgeEvent
        On any signed bridge webhook (state, go-to, battery, online), including a dropped duplicate
            - delivery becomes confirmed
            - lastBridgeEventAt is now
    tickLocal
        While delivery is unconfirmed
            - reads /status every Liveness (1 min), not more often
            - applies the read under the hint rules (a recent webhook-set bolt is not overridden)
            - asks the cloud for nothing (fake cloud calls unchanged)
        While delivery is confirmed
            - reads /status only at the reconcile interval (and the existing confirm/unknown triggers)
        While registration keeps failing
            - retries registration every WebhookRetry (10 min)
            - reads /status every Liveness (no separate 10 min poll)
    Detector (a): /status shows a different bolt state
        With delivery confirmed and no bridge webhook within Reconcile
            - delivery becomes unconfirmed
            - one warning with address "10.0.0.5:8099"
        With a bridge webhook within Reconcile
            - delivery stays confirmed; no warning
        With the current bolt state unknown
            - delivery stays confirmed; no warning
    Detector (b): a command sent via the bridge
        With no bridge webhook from sentAt to sentAt + WebhookConfirm (confirmed via the cloud meanwhile)
            - delivery becomes unconfirmed at sentAt + WebhookConfirm, with one warning
        With a bridge webhook after sentAt
            - delivery stays confirmed
        For a command sent via the cloud
            - no check is scheduled
    Warning
        Repeated every minute for 30 min
            - logged once, then once more after 10 min with repeated=N (s.warn rate limit)
```

Wiring notes, in prose:
- **Unconfirmed reads.** They are a new `tickLocal` step, placed before liveness. When unconfirmed and `now ≥ nextUnconfirmedRead`, it reads `/status` (`reconcile`) and schedules the next read at `now + Liveness`.
- **Registration-pending step.** It keeps retrying registration every `WebhookRetry`, but no longer calls `reconcile` itself.
- **Detector (a).** It runs where a successful `/status` result is compared with the current state, before `applyStatus` changes it. The time since the last bridge webhook uses `lastBridgeEventAt`; a zero value counts as "none".
- **Detector (b).** It is scheduled in `sent` for `via local`: `bridgeCheckSince = now` and `bridgeCheckAt = now + WebhookConfirm`. A `tickLocal` step checks it. The check is a supervisor timer, not a command field, so it survives the command being confirmed early and cleared.
- **Unconfirmed while unconfirmed.** Calling `unconfirmWebhooks` while already unconfirmed only re-warns, and that goes through the rate limit.

- [ ] **Step 1:** Write the tests:
  - `TestEnteringLocalLeavesDeliveryUnconfirmed`;
  - `TestBridgeWebhookConfirmsDelivery`;
  - `TestDuplicateBridgeWebhookConfirmsDelivery`;
  - `TestUnconfirmedReadsStatusEveryLiveness`;
  - `TestConfirmedReadsStatusOnlyAtReconcile`;
  - `TestUnconfirmedReadsUseNoCloudBudget`;
  - `TestCreatedWebhookResetsConfirmation`;
  - `TestExistingWebhookKeepsConfirmation`;
  - `TestRegistrationFailureReadsStatusEveryLiveness`;
  - `TestMissedChangeUnconfirmsDelivery`;
  - `TestChangeWithRecentBridgeWebhookKeepsConfirmation`;
  - `TestUnknownBoltDoesNotUnconfirm`;
  - `TestLocalCommandWithoutBridgeWebhookUnconfirms`;
  - `TestLocalCommandWithBridgeWebhookKeepsConfirmation`;
  - `TestCloudCommandSchedulesNoBridgeCheck`;
  - `TestUnconfirmedWarningIsRateLimited`.

  The warning tests capture logs with a `slog` handler that writes into a buffer, like `logBuffer` in `internal/mqtt/cloud_webhook_test.go`. Pass it via the `tweak` on `Deps.Log`.
- [ ] **Step 2:** Run `go test ./internal/gateway -run 'Delivery|Unconfirm|Confirmation|ReadsStatus|BridgeCheck|RegistrationFailure|MissedChange|UnknownBoltDoesNot'`. Expected: build failure on the new fields, or FAIL. Record the list.
- [ ] **Step 3:** Implement the BDD skeleton following the wiring notes. Update the registration-failure warning text to "…; reading /status every minute until it works".
- [ ] **Step 4:** Run `go test -race ./internal/gateway`. Expected: PASS. Existing tests that counted `/status` calls may now see the 1-minute reads. For each one, fix the test setup (deliver a bridge webhook first, to confirm delivery). Never change the production rule to fit an old count. Ledger each such test as a ruling.
- [ ] **Step 5:** In `addon/DOCS.md` setup step 4, replace "Without that rule, state changes arrive only from occasional polls." with: "Without that rule the add-on log warns that bridge webhooks are not arriving (naming the address and port to allow), and the gateway reads the bridge's status every minute instead, so changes show up late and without lock events."
- [ ] **Step 6:** Run `go test -race ./...`. Expected: PASS. The app end-to-end tests post signed bridge webhooks, so they confirm delivery.
- [ ] **Step 7:** Commit with the message `gateway: confirm bridge webhook delivery instead of assuming it`.

### Task 7: Whole-branch verification

**Files:** none (verification only). Update the memory file `go-loqed-spec-2-1-pending.md` with the new HEAD and the rev 2.3 status.

No BDD skeleton: this task only runs checks.

- [ ] **Step 1:** Run `gofmt -l .`. Expected: no output.
- [ ] **Step 2:** Run `go vet ./...`. Expected: no output.
- [ ] **Step 3:** Run `go test -race ./...`. Expected: all packages `ok`.
- [ ] **Step 4:** Run `go test -race -count=5 ./internal/gateway/... ./internal/app/...`. Expected: all `ok` (timing-sensitive tests are stable).
- [ ] **Step 5:** Run `go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.14.0 run ./...`. Expected: `0 issues.`
- [ ] **Step 6:** Update memory. Nothing to commit unless an earlier step changed files.

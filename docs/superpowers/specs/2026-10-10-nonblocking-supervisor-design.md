# loqed-mqtt: non-blocking supervisor loop — Design

Date: 2026-10-10 (revised 2026-10-16)
Status: Draft, for review
Findings: ARCH H1 (blocking I/O on the per-lock goroutine), COR M1 (an attempt can start after its deadline); see `.superpowers/sdd/2026-10-10-project-review/`.
Builds on: `2026-10-04-loqed-mqtt-gateway-design.md` (the gateway spec; sections referenced as "gateway 5.6" etc.).

**Why this and not the Elm redesign.** The Elm-style redesign (`2026-10-10-elm-architecture-design.md`, branch `spec/elm-architecture`) was reviewed adversarially twice. It fixes the same two bugs but rewrites all of `internal/gateway`, and most of its complexity came from making every synchronous chain asynchronous; each review found more of those cases. This design fixes H1 and M1 with a small, contained change and keeps H1/M1 out of production sooner. The Elm work is parked: if the structure (ARCH H2) still hurts after in-place extractions, the account side (`Budget`, `CloudHub`, `Refresher`) is the first candidate for a reducer, with the output bus and time helpers from that spec.

## 1. Problem

Each lock has one supervisor goroutine (`Supervisor.Run`) that handles commands, bridge and cloud events, and ticks. Two kinds of waiting on it are not the lock's own work and can delay commands, including across locks:

| | What blocks | Worst case today | Crosses locks |
|---|---|---|---|
| A | MQTT publishes: `Client.publish` waits up to 5 s for PUBACK; paho's own `Publish` can block up to 30 s handing off to its writer when the socket is stuck (`WriteTimeout` unset); `retainMu` is held across the wait, and the reconnect republish holds it across N×4 publishes | 35 s per publish, longer behind `retainMu` | Yes (`retainMu`) |
| B | Cloud reads: `CloudHub.Locks` (polls, confirmation reads) waits on the hub-wide read semaphore, may mint a token (20 s) and runs up to two ListLocks (15 s each) | ~50 s | Yes (semaphore) |

Consequences: a slow or half-dead broker, or another lock's slow cloud read, delays a lock's commands and events; an OPEN can reach the lock after its 10 s deadline (COR M1), because the deadline is not re-checked after the `sending` publish.

## 2. Goal and success criteria

The per-lock loop never waits for MQTT acknowledgements or for cloud reads. A command waits at most for its own lock's bridge call (or cloud command) already in progress. No attempt starts after its cutoff or deadline, measured on the monotonic clock.

- A broker that withholds PUBACKs does not delay any lock's command or event handling.
- One lock's slow cloud read does not delay another lock's commands, events or reads, and does not delay its own commands.
- With a stuck MQTT socket, a publish blocks its caller for at most `WriteTimeout` (2 s) per handoff.
- A wall-clock step (NTP correction, a host booting without RTC) changes no deadline, cutoff, timeout or window measured inside the process.
- All safety invariants in CLAUDE.md hold unchanged; MQTT topics and payloads are unchanged.

## 3. Non-goals (deliberately unchanged)

- Bridge HTTP calls and the cloud command stay synchronous on the loop: they are the lock's own work, one at a time by design, and already bounded (5 s; the command deadline).
- Credential refresh (`refreshAndRebuild`) stays synchronous: its result decides the next step at once (new IP → retry local; rebuild the client), it is throttled (5 min to 6 h) and only runs while the lock is already failing. Trade-off: it can still wait on the hub's semaphore behind another lock's read (≤ ~50 s), affecting only the refreshing lock.
- Budget accounting, the hub's read semaphore and its 30 s result sharing (gateway 5.6); each caller keeps its own budget check at its own priority.
- No outbound queue of our own: paho's writer goroutine is the queue.
- Persisted timestamps (budget calls, `blocked_until`, last mint, cache `fetched_at`) stay wall-clock values with today's restore rules; they must survive restarts, which a monotonic reading cannot.
- Not in scope, tracked in the findings ledger: COR M2 (cloud-first `GO_TO_STATE` skips the `/status` fallback), COR M3 (401 + failed re-mint classified `no_response`), COR L1–L4.

## 4. Design

### 4.1 MQTT publishing (`internal/mqtt`)

1. **Bounded writes.** The client options set `SetWriteTimeout(2 * time.Second)`. paho uses it both for the socket write deadline and for handing a publish to its writer, so a stuck connection costs a caller at most 2 s per handoff instead of 30 s. paho's failed write closes the connection; from then on the not-connected guard (item 3) drops publishes at once.
2. **Lock publishes do not wait for PUBACK.** `PublishState`, `PublishEvent`, `PublishAvailability`, `PublishCommandStatus`, `PublishWebhooks` and `PublishWebhooksResult` hand the message to paho and return. Their `error` result now means only what is known at once: a marshal error, or a handoff that paho failed immediately. The `gateway.Publisher` interface and every call site stay as they are.
3. **Not connected → not sent** (today's guard, kept): while paho reports the connection closed, nothing is handed to paho; retained values are still cached and go out with the reconnect republish. QoS 1 events are therefore never stored for replay (paho also resets its store on reconnect with `CleanSession(true)`).
4. **Outcome logging.** For each lock publish the client starts a goroutine that waits for the paho token (`Done()`, at most `publishTimeout` 30 s, or `Close`). On an error or a timeout it logs a rate-limited warning `publishing <kind> failed`, naming the topic kind, never the payload. Nothing in the gateway waits for or reacts to the outcome. Each goroutine ends at PUBACK, on a paho error, after 30 s, or at `Close`.
5. **Waiting publishes stay where a result is needed.** The gateway `online` status, discovery documents, removed-lock clean-up (which decides "cleared" from the result) and `Close`'s `offline` status keep waiting for their acknowledgement as today. None of them runs on a supervisor goroutine.
6. **Per-topic retained consistency.** `retainMu` only covers "update the cache, hand the message to paho" for one topic. The reconnect republish no longer holds it across the whole loop: for each retained topic it takes the lock, reads the topic's *current* cached value and hands it to paho, then releases. A newer value published concurrently is therefore never overtaken by a stale one: whichever handoff comes last carries the newest cached value.
7. **Ordering.** paho has one writer, so messages leave in handoff order. A lock publishes from its own goroutine, so its messages keep their order.
8. **Close.** `Close` publishes the retained `offline` status and waits for that publish for at most 2 s (as today), then disconnects with a short quiesce so paho can flush what its writer holds.

### 4.2 Cloud reads (`internal/gateway`)

1. **One read in flight per lock.** `Supervisor.requestCloud(p Priority, notBefore time.Time)` replaces the direct `pollCloud` calls in `enterCloud`, `tickCloud` (background poll) and `tick` (confirmation poll). If no read is in flight it starts one through `Deps.Go` (default: a plain goroutine) that calls `Deps.Cloud.Locks(ctx, p, notBefore)` and sends the result to a result channel with buffer 1 — the read's future — so the goroutine can always finish, even after the supervisor stopped.
2. **Loop.** `Run` selects on the in-flight read's channel next to `s.in` and the timers (a nil channel when nothing is in flight). The result is applied on the loop by the existing logic (today's `pollCloud` after the call, renamed `applyCloudRead`): error classes, `markStale`, `cloudAPIFailures`, offline transition, `applyCloudLock`. Then the in-flight read is cleared and a queued request, if any, starts.
3. **Requests while a read is in flight** are merged into one queued request, started when the current read finishes: a confirmation read wins over a background poll; for two confirmation reads the later `notBefore` wins. A queued background poll is dropped when a confirmation read is queued. Each started read goes through `CloudHub.Locks` with its own priority, exactly as today.
4. **Freshness across feeds.** A read now finishes later than the events around it, so it can carry data older than a `/status` read made meanwhile (for example after the lock went back to local mode). `applyCloudLock` therefore sets the bolt state and clears `state_stale` only if the list was fetched at or after both `lastEventAt` (today's guard) and `lastStatusAt`, the start time of the last successful `/status` read, which `readStatus` and `tryEnterLocal` now record. Battery and online status apply as today. A confirmation read that the guard rejects counts as "not yet showing the target" (today's lagging-cloud branch).
5. **Mode changes while a read is in flight** need no further special case: the result is applied with today's rules in whatever mode the lock is in when it arrives, behind the guard of item 4. `ModeOffline` → `ModeCloud` on fresh data stays as is.
6. **Shutdown.** Cancelling the context ends `CloudHub.Locks`; the goroutine writes into the buffered channel and exits; `Run` has returned and the result is discarded.
7. **Freshness tracking.** A read in flight does not count as fresh data; `checkFreshness` and `state_stale` behave as today.

### 4.3 Deadline re-check (COR M1, `pipeline.go`)

Immediately before the network call, `attemptLocal` and `attemptCloud` read the clock again (after the `sending` publish). If the local cutoff (local attempt) or the deadline (cloud attempt) has passed, the attempt is not made: the command fails `expired` with the existing message, exactly like the checks before the attempt. Request contexts use `context.WithDeadline` with absolute times: local `min(now+RequestTimeout, cutoff)`, cloud `deadline`. "No attempt starts after its deadline" (gateway 5.8) then holds regardless of how long anything before the call took; a request that started in time may still finish after the deadline, as today.

### 4.4 Monotonic time for every in-process duration

Go's `time.Now()` carries a monotonic clock reading, and `Sub`, `Before`, `After`, `Until` and `context.WithDeadline` use it when both operands have one. A value loses it when it passes through `UTC()`, `Local()`, `In()`, `Round()`, `Truncate()`, `time.Unix()`, parsing or persistence. The rules:

1. **Source.** The injected clocks (`Deps.Now`, `mqtt.ClientConfig.Now`, `Budget`'s and `CloudHub`'s `now`) are `time.Now` in production, passed unmodified. The command arrival time (`mqtt.Command.At`) is taken from that clock, so `receivedAt`, `deadline` and `cutoff` carry the monotonic reading.
2. **Derivation.** Every deadline, cutoff, timeout, backoff, retry time, confirmation window and freshness comparison is derived from such a value with `Add` only, and compared with values from the same clock. Request contexts use `context.WithDeadline` with those absolute times (4.3), never `WithTimeout` from a recomputed duration.
3. **Display only.** `UTC()`/`Truncate()` stay where a time is serialized: the state document (`last_event_at`), `command_status` times, the webhook list's `fetched_at`, the cache's `fetched_at`. Such values are never fed back into timing.
4. **Persisted values.** Budget calls, `blocked_until`, the last mint time and the cache's `fetched_at` are wall-clock by necessity and keep today's restore rules (non-goals).
5. **Tests.** Fake clocks return times without a monotonic reading; Go then falls back to wall-clock arithmetic, which is the time the test controls, so existing tests keep working. A test checks that the production wiring's clocks return values with a monotonic reading (`String()` contains `m=`), and that `Command.At` from the MQTT client carries one.

Today's code already follows rules 1–3 everywhere except the M1 contexts (4.3). This section makes them explicit so the async read path and future code keep them.

## 5. Behaviour (BDD)

```
mqtt.Client
    PublishState / PublishEvent / PublishAvailability / PublishCommandStatus / PublishWebhooks / PublishWebhooksResult
        On a connected broker that withholds PUBACK
            - returns without waiting for the PUBACK
        On a PUBACK
            - logs nothing
        On a publish error or no PUBACK within 30 s
            - logs a rate-limited warning naming the topic kind, never the payload
        While disconnected
            - hands nothing to paho and returns nil
            - a retained value is cached and sent by the next reconnect republish
        On a marshal error
            - returns that error
    gateway status, discovery, removed-lock clean-up, Close
        - wait for their acknowledgement as today (Close at most 2 s)
    client options
        - set WriteTimeout to 2 s
    reconnect republish
        On a retained value published concurrently with the republish
            - the broker's last message on that topic is the newest value
        - never holds retainMu across more than one topic's handoff
    clock
        - Command.At carries a monotonic clock reading in production

gateway.Supervisor
    requestCloud
        On no read in flight
            - starts one read via Deps.Go with the given priority and notBefore
        On a read in flight
            - starts no second read
            - queues the request; a confirmation request replaces a queued background poll
            - for two confirmation requests keeps the later notBefore
        On the in-flight read finishing
            - applies the result on the loop with today's rules
            - starts the queued request, if any
    applyCloudLock
        On a list fetched at or after lastEventAt and lastStatusAt
            - applies the bolt state and clears state_stale (today's rules)
        On a list fetched before the last /status read started
            - applies battery and online only; the bolt state and state_stale stay
            - a confirmation read counts as not yet showing the target
    readStatus / tryEnterLocal
        On success
            - records lastStatusAt as the time the read started
    Run with a read in flight
        - handles commands, bridge and cloud events, and ticks meanwhile
        On context cancel
            - returns; the read's goroutine finishes without blocking
    two locks sharing a CloudHub
        On lock A's read blocked in the cloud API
            - lock B's commands, events and its own reads proceed
            - lock A's commands and events proceed
    result after a mode change
        - is applied with today's rules behind the freshness guard

commandPipeline
    attemptLocal
        On the local cutoff passing during the sending publish
            - makes no bridge call; the command fails expired
    attemptCloud
        On the deadline passing during the sending publish
            - makes no cloud call; the command fails expired
    request contexts
        - are created with context.WithDeadline at min(now+RequestTimeout, cutoff) (local) and at the deadline (cloud)

app wiring
    - passes time.Now unmodified as every injected clock
```

Existing tests stay unchanged and green, in particular every safety invariant test (no resend after `ErrNoResponse`, retained inputs ignored, budget, SetWebhooks never during a command).

## 6. Testing approach

- Gateway tests keep the injectable clock and fakes; `Deps.Go` in the harness queues read tasks that the test runs explicitly, so read completion order is deterministic and no test sleeps. Held reads cover: a background poll answered after a newer `/status` (freshness guard), a confirmation read answered after the lock went back to local mode, a command arriving while a read is in flight.
- The cross-lock test runs two supervisors against the real `CloudHub` + `Budget` with only the HTTP API faked (one lock's read blocks on a channel the test controls).
- MQTT tests use the in-process broker (`internal/testutil`); withheld PUBACKs via a broker hook; the republish race uses an injected paho client that records handoff order. Outcome warnings are asserted through a captured logger.
- Monotonic-clock tests: the app wiring check and the `Command.At` check of 4.4.
- `go test -race -count=5` for `internal/gateway` and `internal/mqtt`.

## 7. Documentation

- Gateway spec: a short "Concurrency" subsection (what may block a lock's loop: its own bridge call and cloud command; what may not: MQTT acknowledgements, cloud reads); updates to the MQTT client section, 5.6 (reads are asynchronous to the loop; freshness across feeds) and 5.8 (deadline re-check right before the call; monotonic time).
- CLAUDE.md, safety invariants: "The per-lock loop never waits for MQTT acknowledgements or for cloud reads." and "Deadlines, cutoffs and timeouts are measured on the monotonic clock: never derive them from `UTC()`, `Truncate()`, parsed or persisted times."
- CHANGELOG `[Unreleased]` → Fixed: a slow MQTT broker or one lock's slow cloud read no longer delays lock commands, and a command is never sent after its deadline.

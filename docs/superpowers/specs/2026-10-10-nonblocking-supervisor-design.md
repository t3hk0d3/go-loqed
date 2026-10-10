# loqed-mqtt: non-blocking supervisor loop — Design

Date: 2026-10-10
Status: Draft, for review
Findings: ARCH H1 (blocking I/O on the per-lock goroutine), COR M1 (an attempt can start after its deadline); see `.superpowers/sdd/2026-10-10-project-review/`.
Builds on: `2026-10-04-loqed-mqtt-gateway-design.md` (the gateway spec; sections referenced as "gateway 5.6" etc.).

## 1. Problem

Each lock has one supervisor goroutine (`Supervisor.Run`) that handles commands, bridge and cloud events, and ticks. Two kinds of waiting on it are not the lock's own work and can delay commands, including across locks:

| | What blocks | Worst case today | Crosses locks |
|---|---|---|---|
| A | MQTT publishes: `Client.publish` waits up to 5 s for PUBACK; paho's own `Publish` can block up to 30 s handing off to its writer when the socket is stuck (`WriteTimeout` unset); `retainMu` is held across the wait, and the reconnect republish holds it across N×4 publishes | 35 s per publish, longer behind `retainMu` | Yes (`retainMu`) |
| B | Cloud reads: `CloudHub.Locks` (polls, confirmation reads) waits on the hub-wide read semaphore, may mint a token (20 s) and runs up to two ListLocks (15 s each) | ~50 s | Yes (semaphore) |

Consequences: a slow or half-dead broker, or another lock's slow cloud read, delays a lock's commands and events; an OPEN can reach the lock after its 10 s deadline (COR M1), because the deadline is not re-checked after the `sending` publish.

## 2. Goal and success criteria

The per-lock loop never waits for MQTT acknowledgements or for cloud reads. A command waits at most for its own lock's bridge call (or cloud command) already in progress. No attempt starts after its cutoff or deadline.

- A broker that withholds PUBACKs does not delay any lock's command or event handling.
- One lock's slow cloud read does not delay another lock's commands, events or reads, and does not delay its own commands.
- With a stuck MQTT socket, a publish blocks its caller for at most `WriteTimeout` (2 s).
- All safety invariants in CLAUDE.md hold unchanged; MQTT topics and payloads are unchanged.

## 3. Non-goals (deliberately unchanged)

- Bridge HTTP calls and the cloud command stay synchronous on the loop: they are the lock's own work, one at a time by design, and already bounded (5 s; the command deadline).
- Credential refresh (`refreshAndRebuild`) stays synchronous: its result decides the next step at once (new IP → retry local; rebuild the client), it is throttled (5 min to 6 h) and only runs while the lock is already failing. Trade-off: it can still wait on the hub's semaphore behind another lock's read (≤ ~50 s), affecting only the refreshing lock.
- Budget accounting, the hub's read semaphore and its 30 s result sharing (gateway 5.6).
- No outbound queue of our own: paho's writer goroutine is the queue, its `Token` the future.

## 4. Design

### 4.1 MQTT publishing (`internal/mqtt`)

1. **Bounded writes.** The client options set `SetWriteTimeout(2 * time.Second)`. paho uses it both for the socket write deadline and for handing a publish to its writer, so a stuck connection costs a caller at most 2 s instead of 30 s. Keep-alive (30 s) then replaces the connection.
2. **No PUBACK waits on callers.** `publish` calls paho `Publish` and returns. An error already set on the returned token (not connected, handoff timeout) is logged, rate-limited; nothing waits for completion.
3. **Delivery handles.** Every `Publish*` method returns a `*Delivery` with `Done() <-chan struct{}` and `Err() error`, wrapping paho's token: done on PUBACK (`Err` nil) or on failure (`Err` set). While the connection is not open, nothing is sent and the returned Delivery is already done with `ErrNotConnected`; retained values are still cached and go out with the reconnect republish. A marshal error returns an already-done Delivery with that error.
4. **Per-topic retained consistency.** `retainMu` only covers "update the cache, hand the message to paho" for one topic. The reconnect republish no longer holds it across the whole loop: for each retained topic it takes the lock, reads the topic's *current* cached value and hands it to paho, then releases. A newer value published concurrently is therefore never overtaken by a stale republished one: whichever handoff comes last carries the newest cached value.
5. **Ordering.** paho has one writer, so messages leave in handoff order. A lock publishes from its own goroutine, so its messages keep their order.
6. **Gateway interface.** `gateway.Publisher` methods return `gateway.Delivery`, a structural interface `{ Done() <-chan struct{}; Err() error }` declared in `gateway`; `*mqtt.Delivery` satisfies it without either package importing the other (the `internal/mqtt` ↔ `gateway` boundary in CLAUDE.md is unchanged). Supervisors ignore the handles. Test fakes return already-done handles.
7. **Close.** `Close` publishes the retained `offline` status and waits for that Delivery for at most 2 s (as today), then disconnects with a short quiesce so paho can flush what its writer holds.

### 4.2 Cloud reads (`internal/gateway`)

1. **One read in flight per lock.** `Supervisor.requestCloud(p Priority, notBefore time.Time)` replaces the direct `pollCloud` calls in `enterCloud`, `tickCloud` (background poll) and `tick` (confirmation poll). If no read is in flight it starts one through `Deps.Go` (default: a plain goroutine) that calls `Deps.Cloud.Locks(ctx, p, notBefore)` and sends the result to a result channel with buffer 1 — the read's future — so the goroutine can always finish, even after the supervisor stopped.
2. **Loop.** `Run` selects on the in-flight read's channel next to `s.in` and the timers (a nil channel when nothing is in flight). The result is applied on the loop by the existing logic (today's `pollCloud` after the call, renamed `applyCloudRead`): error classes, `markStale`, `cloudAPIFailures`, offline transition, `applyCloudLock` with its fetched-at guard (a poll never overwrites newer webhook state). Then the in-flight read is cleared.
3. **Requests while a read is in flight** are merged into one queued request, started when the current read finishes: a confirmation read wins over a background poll; for two confirmation reads the later `notBefore` wins. A queued background poll is dropped when a confirmation read is queued.
4. **Mode changes while a read is in flight** need no special case: the result is applied with today's rules in whatever mode the lock is in when it arrives (the fetched-at guard covers stale data; `ModeOffline` → `ModeCloud` on fresh data stays as is).
5. **Shutdown.** Cancelling the context ends `CloudHub.Locks`; the goroutine writes into the buffered channel and exits; `Run` has returned and the result is discarded.
6. **Freshness.** A read in flight does not count as fresh data; `checkFreshness` and `state_stale` behave as today.

### 4.3 Deadline re-check (COR M1, `pipeline.go`)

Immediately before the network call, `attemptLocal` and `attemptCloud` read the clock again (after the `sending` publish). If the local cutoff (local attempt) or the deadline (cloud attempt) has passed, the attempt is not made: the command fails `expired` with the existing message, exactly like the checks before the attempt. Request contexts use `context.WithDeadline` with absolute times: local `min(now+RequestTimeout, cutoff)`, cloud `deadline`. "No attempt starts after its deadline" (gateway 5.8) then holds regardless of how long anything before the call took; a request that started in time may still finish after the deadline, as today.

## 5. Behaviour (BDD)

```
mqtt.Client
    Publish* (State, Event, Availability, CommandStatus, Webhooks, WebhooksResult)
        On a connected broker that withholds PUBACK
            - returns without waiting for the PUBACK
            - the returned Delivery is not done until the PUBACK arrives
        On a PUBACK
            - Delivery is done with Err() == nil
        While disconnected
            - publishes nothing; Delivery is done with ErrNotConnected
            - a retained value is cached and sent by the next reconnect republish
        On a marshal error
            - Delivery is done with that error
    client options
        - set WriteTimeout to 2 s
    reconnect republish
        On a retained value published concurrently with the republish
            - the broker's last message on that topic is the newest value
        - never holds retainMu across more than one topic's handoff
    Close
        - publishes the retained offline status and waits at most 2 s for it

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
    Run with a read in flight
        - handles commands, bridge and cloud events, and ticks meanwhile
        On context cancel
            - returns; the read's goroutine finishes without blocking
    two locks sharing a CloudHub
        On lock A's read blocked in the cloud API
            - lock B's commands, events and its own reads proceed
            - lock A's commands and events proceed
    result after a mode change
        - is applied with today's rules; data fetched before newer webhook state never overwrites it

commandPipeline
    attemptLocal
        On the local cutoff passing during the sending publish
            - makes no bridge call; the command fails expired
    attemptCloud
        On the deadline passing during the sending publish
            - makes no cloud call; the command fails expired
    request contexts
        - end at min(now+RequestTimeout, cutoff) (local) and at the deadline (cloud)
```

Existing tests stay unchanged and green, in particular every safety invariant test (no resend after `ErrNoResponse`, retained inputs ignored, budget, SetWebhooks never during a command).

## 6. Testing approach

- Gateway tests keep the injectable clock and fakes; `Deps.Go` in the harness queues read tasks that the test runs explicitly, so read completion order is deterministic and no test sleeps.
- The cross-lock test runs two supervisors against the real `CloudHub` + `Budget` with only the HTTP API faked (one lock's read blocks on a channel the test controls).
- MQTT tests use the in-process broker (`internal/testutil`); withheld PUBACKs via a broker hook; the republish race uses an injected paho client that records handoff order. Waiting on Delivery handles replaces fixed sleeps where a test needs "published".
- `go test -race -count=5` for `internal/gateway` and `internal/mqtt`.

## 7. Documentation

- Gateway spec: a short "Concurrency" subsection (what may block a lock's loop: its own bridge call and cloud command; what may not: MQTT acknowledgements, cloud reads); updates to the MQTT client section, 5.6 (reads are asynchronous to the loop) and 5.8 (deadline re-check right before the call).
- CLAUDE.md, safety invariants: "The per-lock loop never waits for MQTT acknowledgements or for cloud reads."
- CHANGELOG `[Unreleased]` → Fixed: a slow MQTT broker or one lock's slow cloud read no longer delays lock commands, and a command is never sent after its deadline.

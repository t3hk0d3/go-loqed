# go-loqed

- **GoLoqed** (`bridge`, `cloud`, `cloud/portal`): a Go client for the LOQED
  local Bridge API, the cloud Lock API and the Integrations portal.
- **loqed-mqtt** (`cmd/loqed-mqtt`): a local-first MQTT gateway for LOQED
  locks with Home Assistant discovery and automatic cloud fallback.

## Why loqed-mqtt instead of the built-in integration

- **More reliable.** Careful retries and automatic cloud fallback when the
  bridge drops off Wi-Fi, without going over LOQED's limit of 12 status reads
  per 12 hours. You don't need retry or fallback logic in your automations.
- **Safer commands.** A command counts as done only when the lock confirms
  it, and `OPEN` is never sent twice. Your automations can act on a confirmed
  result or a `command_failed` event instead of hoping the door locked.
- **More informative.** Who or which key last used the lock, every lock
  event, Wi-Fi and battery details, connection mode, and a flag when the
  state may be out of date.
- **Low maintenance.** A new bridge IP or new keys are picked up
  automatically, the access token renews itself (with email and password
  set), and one instance covers every lock on the account.
- **Local and open.** Bridge events stay on your network even with Home
  Assistant Cloud. Any MQTT client can use the locks, and the gateway keeps
  tracking them while Home Assistant restarts.

The core integration needs no MQTT broker. Use one or the other per lock:
every extra webhook on the bridge delays events.

## Running loqed-mqtt

Home Assistant OS: add this repository in *Settings → Add-ons → Add-on
store → Repositories* and install **LOQED MQTT Gateway** (amd64, aarch64).

Docker: see `docker-compose.yml` (host networking recommended). Without host
networking the auto-detected webhook address is the container's, which the
bridge cannot reach: set `LOQED_WEBHOOK__PRIVATE_URL` to
`http://<docker-host-ip>:8099` and publish port 8099. The Docker
`HEALTHCHECK` reads only environment and `/data/options.json`, so set a
non-default listen address with `LOQED_WEBHOOK__LISTEN`.

Configuration comes from `/data/options.json`, an optional YAML file
(`--config`), and `LOQED_*` environment variables (nested keys use `__`,
for example `LOQED_MQTT__BASE_TOPIC`), in increasing order of precedence.
See `docs/superpowers/specs/2026-10-04-loqed-mqtt-gateway-design.md` for
every setting.

## MQTT topics

| Topic | Retained | Payload |
|---|---|---|
| `loqed/status` | yes | `online` / `offline` |
| `loqed/<id>/availability` | yes | `online` / `offline` |
| `loqed/<id>/state` | yes | JSON state document |
| `loqed/<id>/event` | no | JSON lock event (incl. `command_failed`) |
| `loqed/<id>/command` | must not be retained | `LOCK`, `UNLOCK` or `OPEN`, or JSON `{"command":"LOCK","id":"my-id"}` |
| `loqed/<id>/command_status` | yes | JSON status of the last command |
| `loqed/<id>/cloud_webhook` | must not be retained | a LOQED cloud webhook body, forwarded unchanged (only with `mqtt.cloud_webhooks: true`) |

Retained messages on the `command` and `cloud_webhook` topics are ignored.

`cloud_webhook` lets a relay, typically a Home Assistant automation with a
Home Assistant Cloud (Nabu Casa) webhook trigger, deliver LOQED cloud
webhooks without a reverse proxy. The add-on documentation
(`addon/DOCS.md`, "Cloud webhooks") has the automation. The topic's `<id>`
selects the lock, exactly like the per-lock `/cloud/<secret>/<id>` URL; a
body for another lock is dropped with a warning.

`command_status` follows each command:

```json
{"command":"LOCK","id":"my-id","status":"confirmed","via":"local","attempts":1,"error":null,
 "received_at":"2026-10-06T12:00:00.000Z","updated_at":"2026-10-06T12:00:09.412Z"}
```

- `status` goes `pending` → `sending` → `sent` → `accepted` → `confirmed`, or
  ends `failed`, `expired` or `superseded`.
- `sent` only means the bridge or cloud received the request. The bridge
  answers every request, even one the lock will reject. A command counts as
  `accepted` when the lock starts moving with the gateway's key, and as
  `confirmed` when it reaches the target.
- `error` is set for `failed`: `unreachable`, `no_response`, `rejected`,
  `unauthorized`, `key_deleted`, `rate_limited`, `stalled`, `no_confirmation`
  or `offline`.
- The optional `id` (up to 64 printable characters) is echoed back, so an
  automation can match a status to its command.
- Only the latest command counts. A newer command replaces one that has not
  been sent yet, and that one ends as `superseded`. A command the bridge may
  already have received is never sent again.

## Releasing

1. Tag `v<version>` and push the tag. CI tests, then pushes
   `ghcr.io/t3hk0d3/loqed-mqtt` (amd64, arm64, arm/v7) and
   `ghcr.io/t3hk0d3/loqed-mqtt-addon` (amd64, arm64). Prerelease tags
   (`v1.2.0-rc1`) do not move `latest`.
2. First release only: make both GHCR packages public (package settings →
   change visibility), otherwise the Supervisor and `docker pull` fail.
3. After the images exist, bump `version` in `addon/config.yaml` to the same
   version and push. (Bumping first would make add-on updates fail.)

## Development

    go test -race ./...
    go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.14.0 run ./...
    python3 testdata/gen_vectors.py   # regenerate signing golden vectors

# go-loqed

- **GoLoqed** (`bridge`, `cloud`, `cloud/portal`): a Go client for the LOQED
  local Bridge API, the cloud Lock API and the Integrations portal.
- **loqed-mqtt** (`cmd/loqed-mqtt`): a local-first MQTT gateway for LOQED
  locks with Home Assistant discovery and automatic cloud fallback.

## loqed-mqtt vs. the built-in Home Assistant integration

Home Assistant's core `loqed` integration talks only to the bridge. It stores
the bridge's IP and keys once, at setup, reads the bridge's status at
startup, and relies on bridge webhooks after that. loqed-mqtt adds:

- **Cloud fallback.** When the bridge is unreachable, the gateway switches to
  LOQED's cloud API for state and commands, and back to local when the bridge
  returns. It stays under LOQED's limit of 12 status reads per 12 hours (which
  otherwise blocks the account), across restarts too. Cloud webhooks keep the
  state fresh in cloud mode, through a reverse proxy or through Home Assistant
  Cloud and MQTT.
- **Command results you can trust.**
  - The bridge answers "OK" even to commands the lock rejects. The gateway
    counts a command as done only when the lock reports moving and then
    reaching the target.
  - Each command's progress and outcome is published (`command_status`, the
    **Last command** sensor), and a failed command raises a `command_failed`
    event.
  - A command is retried (locally, then once via the cloud) only when it
    provably never reached the bridge, so a slow bridge never gets a second
    `OPEN`.
- **Honest state.** A `state_stale` flag and a **State stale** sensor show
  when the state may be out of date, instead of showing an old state as
  current.
- **More entities.**
  - The core integration has the lock, battery and Bluetooth signal.
  - loqed-mqtt adds Wi-Fi signal, battery voltage, lock online, connection
    mode (local, cloud or offline), last change reason with the key's name,
    last command and token expiry.
  - It also adds an event entity for every lock event, with names for key ids
    (`lock_settings.key_names`).
- **Keeps working when things change.** A new bridge IP (DHCP) or new keys are
  picked up from the cloud automatically, with no need to set the integration
  up again. With email and password configured, the gateway creates and renews
  its own access token; tokens expire after about six months.
- **Bridge webhooks stay on the LAN.** With Home Assistant Cloud active, the
  core integration registers its Nabu Casa URL on the bridge, so every bridge
  event makes a round trip through the internet. The gateway always registers
  its local address.
- **All locks in one place.** One instance serves every lock on the account
  (or an allow-list) and deduplicates the bridge and cloud copies of each
  event.
- **Not tied to Home Assistant.** Any MQTT client (Node-RED, openHAB, scripts)
  can read the retained state and send commands. The gateway keeps running,
  and keeps the state current, while Home Assistant restarts.

What the core integration has that loqed-mqtt doesn't: it needs no MQTT
broker, and it finds bridges via zeroconf. Run one or the other for a lock,
not both: each adds its own webhook to the bridge, and the bridge delivers
webhooks one after another, so every extra target delays events.

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

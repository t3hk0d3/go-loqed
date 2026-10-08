# go-loqed

- **GoLoqed** (`bridge`, `cloud`, `cloud/portal`): a Go client for the LOQED
  local Bridge API, the cloud Lock API and the Integrations portal.
- **loqed-mqtt** (`cmd/loqed-mqtt`): a local-first MQTT gateway for LOQED
  locks with Home Assistant discovery and automatic cloud fallback.

## Why loqed-mqtt instead of the built-in integration

- **More reliable.** Careful retries, and automatic cloud fallback when the
  local network gets in the way (separate VLANs, firewall rules, a bridge that
  changed its IP), without going over LOQED's limit of 12 status reads per 12
  hours. You don't need retry or fallback logic in your automations.
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

## Installation

You need:
- a LOQED lock with a LOQED Bridge;
- an MQTT broker;
- a LOQED **personal access token**, created at
  https://integrations.loqed.com/personal-access-tokens. Instead of a token,
  you can give your LOQED email and password, and the gateway creates and
  renews the token itself.

### Home Assistant OS (add-on)

1. Install the **Mosquitto broker** add-on, if you don't use another broker
   yet, and the **MQTT** integration.
2. Add this repository to the add-on store. Use this button:

   [![Add the repository to your Home Assistant](https://my.home-assistant.io/badges/supervisor_add_addon_repository.svg)](https://my.home-assistant.io/redirect/supervisor_add_addon_repository/?repository_url=https%3A%2F%2Fgithub.com%2Ft3hk0d3%2Fgo-loqed)

   Or add it by hand. Open *Settings → Add-ons → Add-on store* (called *Apps →
   App store* in recent releases), then choose *Repositories* in the ⋮ menu:

   ![The Repositories entry in the add-on store menu](docs/img/hass-addons-repositories.png)

   Paste `https://github.com/t3hk0d3/go-loqed` and choose **Add**:

   ![The Add repository dialog](docs/img/hass-add-repository-dialog.png)

3. Install **LOQED MQTT Gateway**. It runs on amd64 and aarch64.
4. On the add-on's *Configuration* tab, set **Personal access token**, or your
   LOQED email and password.
5. Start the add-on. Each lock appears in Home Assistant as a device, through
   MQTT discovery. The add-on's *Documentation* tab covers every option.

### Docker

The image `ghcr.io/t3hk0d3/loqed-mqtt` runs on amd64, arm64 and arm/v7.
Start from `docker-compose.yml`:

```yaml
services:
  loqed-mqtt:
    image: ghcr.io/t3hk0d3/loqed-mqtt:latest
    restart: unless-stopped
    network_mode: host
    volumes:
      - loqed-data:/data
    environment:
      LOQED_CLOUD_TOKEN: "paste-your-personal-access-token"
      # or: LOQED_CLOUD_EMAIL / LOQED_CLOUD_PASSWORD
      LOQED_MQTT__URL: "tcp://192.168.1.10:1883"
      LOQED_MQTT__USERNAME: "loqed"
      LOQED_MQTT__PASSWORD: "change-me"
volumes:
  loqed-data:
```

Then run `docker compose up -d`. The gateway logs each lock it found.

- **Networking.** Use host networking: the bridge sends its webhooks to the
  gateway's address on port 8099. Without host networking, the address the
  gateway detects is the container's, which the bridge cannot reach. In that
  case, publish port 8099 and set `LOQED_WEBHOOK__PRIVATE_URL` to
  `http://<docker-host-ip>:8099`.
- **Health check.** The image's `HEALTHCHECK` reads only the environment and
  `/data/options.json`. If you change the listen address, set it with
  `LOQED_WEBHOOK__LISTEN`.

### After installing

If the bridge is on another network (an IoT VLAN, for example), allow
connections from the bridge to the gateway on port 8099. Without that rule
the gateway still works, but it reads the bridge's status every minute
instead of receiving events, and its log warns about it.

If the log warns that it "rejected a bridge webhook with a stale timestamp",
the bridge delivered the webhook more than 20 s after signing it. This
happens when the bridge has many registered webhooks, because it calls them
one after another, or when the clocks differ. Remove old webhooks from the
bridge, check NTP, or raise `webhook.bridge_timestamp_tolerance` (for
example `60s`; `0` turns the check off, and a recorded webhook could then be
replayed on your network).

### Configuration

Settings come from three places, each overriding the one before:
1. `/data/options.json` (the add-on options);
2. an optional YAML file (`--config`);
3. `LOQED_*` environment variables. Nested keys use `__`, for example
   `LOQED_MQTT__BASE_TOPIC`.

The design spec, `docs/superpowers/specs/2026-10-04-loqed-mqtt-gateway-design.md`,
lists every setting.

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

Run the release workflow with the new version:

    gh workflow run release -f version=0.1.2

You can also use *Actions → release → Run workflow*. The workflow:

1. runs the tests on `master`;
2. commits the add-on version bump (`addon: release 0.1.2`), tags that
   commit `v0.1.2`, and pushes only the tag;
3. builds and pushes `ghcr.io/t3hk0d3/loqed-mqtt` (amd64, arm64, arm/v7) and
   `ghcr.io/t3hk0d3/loqed-mqtt-addon` (amd64, arm64) from the tag;
4. moves `master` to the tagged commit and creates the GitHub release.

`master` changes last, so Home Assistant never offers an add-on version whose
image does not exist yet. If `master` moved during the run, the workflow
merges the tag into it instead.

A prerelease version (`0.2.0-rc1`) is tagged without an add-on bump. Its
images get only the version tag, so `latest` and add-on users stay on the last
release.

## Development

    go test -race ./...
    go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.14.0 run ./...
    python3 testdata/gen_vectors.py   # regenerate signing golden vectors

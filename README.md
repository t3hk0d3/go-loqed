# go-loqed

- **GoLoqed** (`bridge`, `cloud`, `cloud/portal`): a Go client for the LOQED
  local Bridge API, the cloud Lock API and the Integrations portal.
- **loqed-mqtt** (`cmd/loqed-mqtt`): a local-first MQTT gateway for LOQED
  locks with Home Assistant discovery and automatic cloud fallback.

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
| `loqed/<id>/command` | must not be retained | `LOCK`, `UNLOCK` or `OPEN` |

Retained messages on the command topic are ignored.

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

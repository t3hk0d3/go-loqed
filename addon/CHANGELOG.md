# Changelog

All notable changes to loqed-mqtt, the Home Assistant add-on and the GoLoqed
library are listed here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.2.0] - 2026-10-08

### Changed

- A bridge webhook may now arrive up to 20 s after the bridge signed it
  (was 10 s). A bridge with many registered webhooks calls them one after
  another, so a webhook can arrive late without any clock problem.

### Added

- `webhook.bridge_timestamp_tolerance` sets that limit; `0` turns the
  timestamp check off (the signature is still checked). The warning about a
  stale bridge webhook names the setting.
- Library: `bridge.ParseEventWithin` takes the tolerance; `bridge.MaxClockSkew`
  is now 20 s.
- A **Bridge webhooks** diagnostic sensor and the retained
  `loqed/<id>/webhooks` topic list every webhook the bridge calls (id, URL,
  triggers, and which one is the gateway's own).
- With `mqtt.bridge_webhook_control` (off by default), a request on
  `loqed/<id>/webhooks/set` replaces the bridge's webhook list: listed ones
  are kept or added, the rest removed. It must name the `revision` of the
  list it was made from; the result is published on
  `loqed/<id>/webhooks/result`.
- Library: `bridge.Webhook` carries its `Triggers`; `bridge.ParseTriggers` and
  `Triggers.Names` convert to and from trigger names.

## [0.1.1] - 2026-10-07

### Fixed

- The add-on configuration shows the personal access token, LOQED email and
  LOQED password fields by default. Before, Home Assistant hid them under
  *Show unused optional configuration options*.

## [0.1.0] - 2026-10-07

First release of loqed-mqtt, a local-first MQTT gateway for LOQED locks with
Home Assistant discovery and automatic cloud fallback, and of the GoLoqed
client library (`bridge`, `cloud`, `cloud/portal`).

### Added

- Commands go through the bridge first, with one automatic fallback through
  the cloud. A command is retried only when it provably never reached the
  bridge, so `OPEN` is never sent twice.
- A command counts as done only when the lock reports the move. Progress is
  published on the retained `command_status` topic, and a failure also
  publishes a `command_failed` event.
- A persisted request budget keeps the account under LOQED's limit of 12
  status reads per 12 hours.
- Bridge and cloud webhooks are equal event sources, and an event arriving
  from both is published once. Cloud webhooks can arrive through a reverse
  proxy or be relayed over MQTT by a Home Assistant Cloud automation.
- If the bridge's webhooks don't reach the gateway, it reads the bridge's
  status every minute instead and logs which address and port to allow.
- A changed bridge IP and new keys are picked up automatically. With email
  and password set, the access token renews itself. One instance handles
  every lock on the account.
- Images for amd64, arm64 and arm/v7, and a Home Assistant add-on for amd64
  and aarch64.

### Known issues

- If the lock switches to cloud mode while a cloud command is still being
  confirmed, the command can end `no_confirmation` even though the lock
  moved.
- The Home Assistant OS add-on and the cloud-webhook relay through Home
  Assistant have not been verified end to end yet.

[Unreleased]: https://github.com/t3hk0d3/go-loqed/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/t3hk0d3/go-loqed/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/t3hk0d3/go-loqed/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/t3hk0d3/go-loqed/releases/tag/v0.1.0

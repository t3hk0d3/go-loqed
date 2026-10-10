# Changelog

All notable changes to loqed-mqtt, the Home Assistant add-on and the GoLoqed
library are listed here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Fixed

- A bridge webhook delivered more than once is now applied only once, so a
  repeated delivery can no longer bring back an older lock state or confirm
  a command a second time. This also stops a captured webhook from being
  resent within the timestamp window.

- After a lock command that got no response (it may have moved the lock),
  the state is marked stale, and a delayed bridge status still showing the
  old position no longer clears that: Home Assistant no longer shows, for
  example, a definite "locked" for a door that was unlocked.
- A wrong LOQED token no longer uses up the cloud request budget. LOQED
  does not count requests it rejects, but the gateway did, so a few restarts
  with a mistyped token could keep it out of the cloud for up to 12 hours
  after the token was fixed.

## [0.3.0] - 2026-10-10

### Added

- The project is now licensed under the GNU AGPL v3.0 or later.
- Library: package documentation (authentication, the safety rules for
  commands and retries, rate limits, concurrency) and runnable examples.

### Changed

- Library: the Go library (`github.com/t3hk0d3/go-loqed`) is now its own
  module, without the gateway's dependencies, and works with Go 1.22 and
  newer. Import paths are unchanged. The gateway moved to the
  `github.com/t3hk0d3/go-loqed/loqed-mqtt` module; images and the add-on
  are unchanged.
- Library: the lock keys (`cloud.Lock.KeySecret`, `BridgeKey`, `BackendKey`,
  `bridge.Credentials.BridgeKey`, `KeySecret`) and `portal.Token.Value` are
  now of type `loqed.Secret`. It prints, logs and encodes to JSON as
  `[redacted]`; `Reveal()` (or `string(s)`) returns the value. Decoding is
  unchanged.

### Fixed

- Security hardening: the gateway no longer follows HTTP redirects from the
  bridge. A device that takes over the bridge's IP address can no longer
  bounce signed lock commands or webhook changes to another host; a redirect
  now fails the request, and a lock command that got one is not sent again.
- loqed-mqtt now logs a warning when it cannot connect to the MQTT broker,
  naming the broker (without credentials), the reason (connection refused,
  bad username or password, timeout, TLS, ...) and which settings to check.
  Before, a wrong `mqtt.url` or password, or a broker that was down, logged
  nothing. Repeats of the same reason are logged at most every 10 minutes
  with a count, and the next successful connect reports how many attempts
  failed.
- Lock keys, tokens and passwords no longer show up in full when a lock,
  its credentials or the settings are printed or logged; they appear as
  `[redacted]`. The credential cache (`/data/locks.json`) is unchanged.

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

[Unreleased]: https://github.com/t3hk0d3/go-loqed/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/t3hk0d3/go-loqed/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/t3hk0d3/go-loqed/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/t3hk0d3/go-loqed/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/t3hk0d3/go-loqed/releases/tag/v0.1.0

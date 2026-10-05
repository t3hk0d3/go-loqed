# LOQED MQTT Gateway

Exposes every LOQED lock on your account to Home Assistant through MQTT.
The gateway talks to your LOQED Bridge on the local network and falls back
to the LOQED cloud when the bridge is unreachable.

## Setup

1. Install and start the Mosquitto broker add-on.
2. Create a personal access token at
   https://integrations.loqed.com/personal-access-tokens and paste it into
   **Personal access token**. Alternatively, enter your LOQED email and
   password; the add-on then creates a token named `loqed-mqtt <id>`
   (the password can be removed after the first successful start).
3. Start the add-on. Each lock appears as a device with a lock, battery and
   signal sensors, a connection mode sensor, a last change reason sensor and
   a lock event entity.

## Lock settings

Optional per-lock overrides, edited in YAML mode:

```yaml
lock_settings:
  - lock: Front door          # lock name or id
    bridge_ip: 192.168.1.50   # pin the bridge address
    key_names: "1=Alice,3=Bob"
```

`key_names` maps the lock's key ids to names shown on lock events.
`bridge_key`, `key_secret` and `local_id` are only needed when the LOQED
cloud does not provide local credentials for a lock.

## Things to know

- **Time must be correct.** The bridge signs webhooks with a timestamp that
  must be within 10 seconds of this host's clock.
- **Lock events are best-effort.** The bridge occasionally loses a webhook.
  Use the lock entity, not the event entity, for automations that depend on
  whether the door is locked. Failed commands produce a `command_failed`
  event you can notify on.
- **Cloud limits.** LOQED blocks accounts that read lock status more than 12
  times in 12 hours. The gateway keeps cloud calls under `cloud_budget`
  (default 10, also across restarts), so in cloud mode without cloud
  webhooks the lock state can be more than an hour old. The `state_stale`
  attribute shows when it is.
- **Cloud webhooks (optional).** Set `webhook.public_url` (scheme and host
  only, for example `https://loqed.example.com`) to an address that reaches
  this add-on from the internet through a reverse proxy that forwards only
  the `/cloud/` path, unchanged, and does not log request paths (the path
  contains the secret). The add-on log shows the full URL once at startup;
  register it in the API section of https://app.loqed.com.

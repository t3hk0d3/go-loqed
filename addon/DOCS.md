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
   (keep the password set: the add-on uses it to renew the token before it
   expires, after about six months).
3. Start the add-on. Each lock appears as a device with:
   - a lock entity and a lock event entity;
   - battery and signal sensors;
   - connection mode, last change reason, last command and token expiry
     sensors.
4. If your bridge sits in a separate network (an IoT VLAN, for example),
   allow connections **from the bridge to this host on port 8099**. The
   bridge pushes its webhooks to the gateway. Without that rule the gateway
   reads the bridge's status every minute instead, so changes show up late
   and without lock events; the add-on log warns when it notices that bridge
   webhooks are not arriving, naming the address and port to allow.

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

- **The webhook address is found for you.** For each lock, the gateway asks
  the operating system which of this host's addresses it would use to reach
  that lock's bridge, and registers `http://<that address>:8099/webhook/<lock id>`
  on the bridge. No packets are sent for this. With several networks (an IoT
  VLAN, for example), each bridge gets the address on its own side. The
  address is checked again on every registration check: if this host gets a
  new IP, the old registration is removed from the bridge and the new one
  added. Set `private_url` under **Webhooks** only if the bridge has to
  reach the gateway through another address, such as NAT, a port forward or
  a DNS name (for example `http://ha.lan:8099`).
- **Time must be correct.** The bridge signs webhooks with a timestamp that
  must be within 20 seconds of this host's clock. A bridge with many
  registered webhooks can deliver later than that; the log then warns about
  a stale timestamp. Raise `bridge_timestamp_tolerance` under **Webhooks**
  (for example `60s`), or set it to `0` to turn the check off. With the
  check off the signature is still verified, but a recorded webhook could be
  replayed by someone on your network.
- **Lock events are best-effort.** The bridge occasionally loses a webhook.
  Use the lock entity, not the event entity, for automations that depend on
  whether the door is locked. Failed commands produce a `command_failed`
  event you can notify on.
- **Command results.** The **Last command** sensor (MQTT topic
  `loqed/<id>/command_status`) shows whether a command was confirmed by the
  lock, and if not, why. Only the latest command counts. A command the
  bridge may already have received is never sent again.
- **Keep the bridge's webhook list short.** The bridge delivers webhooks to
  each registered address one after another. Old or unreachable entries,
  for example from earlier Home Assistant setups, delay every event and
  `/status`. The add-on log warns when the bridge has more than three other
  webhooks. The **Bridge webhooks** sensor shows how many there are, and
  its attributes list each one; see [Bridge webhooks](#bridge-webhooks).
- **Revoking access.** Every personal access token gets its own key on the
  lock, and revoking or expiring the token does **not** remove that key. To
  cut the gateway off, delete its key in the LOQED app.
- **Token expiry.** Tokens expire after about six months. The **Token
  expires** sensor shows when, and from two weeks before, the log warns
  daily. With email and password configured, the add-on creates a new token
  by itself.
- **UNLOCK opens the door?** Recalibrate the lock in the LOQED app.
- **Cloud limits.** LOQED blocks accounts that read lock status more than 12
  times in 12 hours. The gateway keeps cloud calls under `cloud_budget`
  (default 10, also across restarts), so in cloud mode without cloud
  webhooks the lock state can be more than an hour old. The `state_stale`
  attribute shows when it is.
- **Cloud webhooks (optional).** LOQED's cloud can call a URL for every lock
  event, which keeps the state fresh when the bridge is unreachable. LOQED
  registers cloud webhooks per lock. There are two ways to receive them; see
  [Cloud webhooks](#cloud-webhooks) below.

## Cloud webhooks

### Through a reverse proxy

Set `webhook.public_url` (scheme and host only, for example
`https://loqed.example.com`) to an address that reaches this add-on from the
internet through a reverse proxy that forwards only the `/cloud/` path,
unchanged, and does not log request paths (the path contains the secret). At
startup the add-on log shows one URL per lock; register each URL for its own
lock in the API section of https://app.loqed.com.

### Through Home Assistant Cloud and MQTT

Use this when you have no reverse proxy, or Home Assistant Cloud (Nabu Casa)
is already set up. A Home Assistant automation receives the webhook and
publishes its body, unchanged, to the lock's `cloud_webhook` MQTT topic.

1. Set `mqtt.cloud_webhooks: true` and restart the add-on. The log shows each
   lock's topic, for example `topic=loqed/Yq1g/cloud_webhook`. The part
   between `loqed/` and `/cloud_webhook` is the lock's **topic id**.
2. Create this automation (one webhook trigger per lock; the trigger `id` is
   that lock's topic id, and each `webhook_id` is a long random string):

   ```yaml
   alias: LOQED cloud webhooks to MQTT
   mode: parallel
   max: 20
   triggers:
     - trigger: webhook
       webhook_id: loqed-front-door-REPLACE-WITH-RANDOM
       id: Yq1g
       allowed_methods: [POST]
       local_only: false
   actions:
     - action: mqtt.publish
       data:
         topic: "loqed/{{ trigger.id }}/cloud_webhook"
         payload: "{{ trigger.json | to_json }}"
         qos: 1
         retain: false
   ```

3. In the automation editor, open each webhook trigger and copy its Home
   Assistant Cloud URL. Register that URL **for the same lock** in the API
   section of https://app.loqed.com.

Things to keep in mind:

- **`retain: false` is required.** Retained `cloud_webhook` messages are
  ignored (a retained body would be replayed as a new event on every
  reconnect); the log says so.
- **One URL per lock.** Don't point one lock's URL at another lock's topic.
  The first webhook on a topic binds that lock to the cloud lock id in the
  body, and the binding is kept across restarts. A later body with a
  different id is dropped with a warning showing both `cloud_lock_id` (the
  body's) and `bound_cloud_lock_id`. If the very first webhook came from the
  wrong lock, correcting the automation is not enough: the lock stays bound
  to the wrong id. To reset it, stop the gateway, remove that lock's
  `cloud_webhook_id` from `/data/locks.json`, and start it again. (Deleting
  the whole file also drops a token the add-on created and the cloud webhook
  URL secret.) In the add-on,
  `/data` is only reachable by reinstalling it, so double-check the mapping
  before registering the URLs.
- **The body is forwarded as LOQED sends it.** It includes your account
  e-mail and name. The add-on never reads or logs those fields, but anything
  else subscribed to `loqed/#` can see them.
- **Trust.** The Home Assistant Cloud URL is the secret: treat it like a
  password. Anyone allowed to publish to `loqed/<id>/cloud_webhook` can
  inject lock events, so restrict it with broker ACLs, as for `command`.
  LOQED does not sign cloud webhooks, so the gateway cannot check where a
  body came from.

## Bridge webhooks

Each lock's bridge webhook list is published, retained, on
`loqed/<id>/webhooks` and shown by the diagnostic **Bridge webhooks**
sensor. It is read while the lock is connected locally: at the gateway's
regular webhook check, when the bridge reports a different number of
webhooks, and after every change made here. `fetched_at` shows its age.

```json
{"revision":"9f2c41d0a1b2c3d4","fetched_at":"2026-10-08T08:00:00Z","count":3,"webhooks":[
  {"id":3,"url":"http://192.168.2.10:8123/api/webhook/abc","triggers":["all"],"gateway":false},
  {"id":5,"url":"https://hooks.nabu.casa/xyz","triggers":["battery","online_status"],"gateway":false},
  {"id":7,"url":"http://192.168.2.20:8099/webhook/QnZk","triggers":["all"],"gateway":true}]}
```

`gateway: true` marks the add-on's own webhook. URLs are shown in full, so
they also end up in Home Assistant's history of the sensor.

### Changing the list

To remove or add webhooks without the LOQED app, turn on
`bridge_webhook_control` under **MQTT** (off by default). There is no Home
Assistant control for it: publish a request, **not retained**, to
`loqed/<id>/webhooks/set`, for example with the MQTT integration's
*Publish a packet* action:

```json
{"revision":"9f2c41d0a1b2c3d4","request_id":"cleanup-1",
 "webhooks":[{"id":3},{"url":"http://192.168.2.11:8123/api/webhook/def","triggers":["all"]}]}
```

- `webhooks` is the **complete** list you want. Every webhook it does not
  mention is **removed** (webhook 5 above). The add-on's own webhook is
  always kept, whether you list it or not.
- `{"id":3}` keeps webhook 3. `{"id":3,"triggers":[...]}` keeps it, and if
  its triggers differ, deletes it and creates it again (it gets a new id).
- `{"url":"...","triggers":[...]}` keeps a webhook with exactly this URL and
  triggers, re-creates it if only the triggers differ, and adds it
  otherwise. Without `triggers` it gets all of them.
- Trigger names: `state_changed_open`, `state_changed_latch`,
  `state_changed_night_lock`, `state_changed_unknown`, `state_goto_open`,
  `state_goto_latch`, `state_goto_night_lock`, `battery`, `online_status`,
  or `all`.
- `revision` must be the one from the list you edited. If the list changed
  since, nothing is changed and the result says `conflict`; the list is read
  again, so take the new revision and try again.
- The whole request is checked before anything changes. A mistake (an
  unknown id, a misspelled trigger, more than 20 entries) changes nothing
  and the result says `invalid` with what is wrong.
- Changes need the bridge (local mode). They run one at a time and never
  delay a lock command by more than one bridge call.

The result is published, **not retained**, on `loqed/<id>/webhooks/result`,
so only a client subscribed at that moment sees it (for example MQTT
Explorer, or *Listen to a topic* in the MQTT integration):

```json
{"request_id":"cleanup-1","status":"ok","error":null,"detail":null,
 "removed":[5],"added":[8],"kept":[3,7],"revision":"0c1d2e3f4a5b6c7d"}
```

`status` is `ok`, `partial` (the bridge refused a call part-way; what was
done is listed) or `failed`. `error` is `invalid`, `conflict`, `offline`,
`unreachable`, `no_response`, `rejected` or `unauthorized`. The webhook list
topic always shows the bridge's real list afterwards.

Anyone allowed to publish to `loqed/<id>/webhooks/set` can change where
your lock's events go. Keep the option off unless you need it, and restrict
the topic with broker ACLs, as for `command`.

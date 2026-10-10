// Package bridge is a stateless client for the local HTTP API of a LOQED
// Bridge: lock commands, the status document, webhook management and
// verification of the webhooks the bridge sends.
//
// # Address and credentials
//
// A bridge is addressed by IP address, optionally with a port (default
// 80); [New] rejects hostnames, so mDNS or DNS answers can never redirect a
// signed command. The address and the [Credentials] come from the cloud
// lock list (cloud.Lock's BridgeIP, BridgeKey, KeySecret and LocalID):
//
//   - BridgeKey authenticates webhook management requests and the webhooks
//     the bridge sends;
//   - KeySecret and LocalKeyID sign lock commands; LocalKeyID is the lock
//     key's id, which the bridge reports back in webhooks
//     (KeyLocalID), so you can tell your own commands apart.
//
// The keys are loqed.Secret values, so printing or logging Credentials
// shows them as "[redacted]".
//
// [Client.Status] works without credentials.
//
// # Commands are confirmed only by webhooks
//
// [Client.Command] returns nil when the bridge answered with HTTP 200. That
// means only that the bridge received the request: it answers 200 to every
// command, also to one the lock will reject. A command is confirmed by the
// webhooks that follow: a [GoToStateEvent] carrying your key id (the lock
// starts moving), then a [StateReachedEvent] (the bolt arrived, or
// MOTOR_STALL). GET /status lags behind and is a hint only.
//
// Send a command again only after an error that wraps loqed.ErrUnreachable
// (the request provably never left). Never resend after
// loqed.ErrNoResponse: the bridge may have received the command, and a
// second OPEN would unlatch the door again. Each Command call signs the
// command afresh with the current time.
//
// # Webhooks
//
// Register a URL with [Client.CreateWebhook]. The bridge POSTs a JSON body
// with the headers HASH and TIMESTAMP; pass them with the body and the
// bridge key ([Client.BridgeKey]) to [ParseEvent], which checks the
// signature, rejects a TIMESTAMP more than [MaxClockSkew] away from now
// ([ParseEventWithin] takes another limit) and decodes the event. The
// bridge calls its webhooks one after another, so every extra registration
// delays the others; remove the ones you no longer need with
// [Client.DeleteWebhook].
//
// # HTTP client
//
// The default HTTP client has a 5 s timeout ([DefaultTimeout]), opens a
// fresh connection per request (no keep-alives) and never follows
// redirects. A client passed to [WithHTTPClient] must keep both:
// DisableKeepAlives on its transport (otherwise net/http may silently
// resend a GET, which here is a signed command, on a reused connection) and
// a CheckRedirect that returns http.ErrUseLastResponse (following a
// redirect re-sends the signed request to another host).
//
// # Concurrency
//
// A [Client] is not changed after [New] and is safe for concurrent use by
// multiple goroutines. The bridge itself drives one lock: sending commands
// concurrently gives no guarantee about their order. [ParseEvent],
// [ParseEventWithin] and [ParseTriggers] are pure functions.
package bridge

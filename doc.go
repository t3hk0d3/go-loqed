// Package loqed is the root of GoLoqed, a Go client library for LOQED
// Touch smart locks. It holds what the clients share: the bolt state, the
// sentinel errors and lenient JSON scalars. The clients live in
// subpackages:
//
//   - [github.com/t3hk0d3/go-loqed/bridge]: the LOQED Bridge's local HTTP
//     API (commands, status, webhook management, incoming webhooks);
//   - [github.com/t3hk0d3/go-loqed/cloud]: the cloud Lock API (lock list
//     with the bridge credentials, cloud commands, cloud webhooks);
//   - [github.com/t3hk0d3/go-loqed/cloud/portal]: the Integrations portal
//     (log in with email and password, create and revoke personal access
//     tokens).
//
// The module uses only the standard library and starts no goroutines.
// Every network method takes a [context.Context] first.
//
// # Typical flow
//
// Create a personal access token at
// https://integrations.loqed.com/personal-access-tokens (or with
// cloud/portal), list the locks with cloud.Client.ListLocks, which returns
// each lock's bridge IP and keys, then talk to the bridge with
// bridge.New. Register a webhook on the bridge and verify every incoming
// webhook with bridge.ParseEvent: webhooks are the only confirmation that
// a lock moved.
//
// # Errors and retries
//
// Clients wrap one of the sentinel errors below; test them with
// [errors.Is] (and [errors.As] for [*APIError]). Error messages never
// contain tokens, keys, signed commands or request URLs.
//
// The safety rule for lock commands: send a command again only after
// [ErrUnreachable], which means the request provably never left. After
// [ErrNoResponse] the bridge or cloud may have received the command and may
// act on it; resending an OPEN could unlatch the door twice. Any answer
// from the server, an error status included, also means the request
// arrived: resend only when the answer says it was rejected (for example
// [ErrUnauthorized]). A retry is a new call; bridge.Client.Command signs
// every call afresh and never replays a request.
//
// # Secrets
//
// Lock keys and token values are [Secret]s (cloud.Lock's KeySecret,
// BridgeKey and BackendKey, bridge.Credentials, portal.Token.Value). A
// non-empty Secret shows as [Redacted] ("[redacted]") wherever it is
// printed: every fmt verb, log/slog (text and JSON handlers), and also as a
// field of a struct or an element of a slice. Encoding it to JSON emits
// "[redacted]" too, so to store a secret, write [Secret.Reveal] (or
// string(s)) into your own field. Decoding JSON into a Secret keeps the
// real value.
package loqed

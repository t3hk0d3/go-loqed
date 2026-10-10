// Package cloud is a stateless client for the LOQED cloud Lock API
// (https://integrations.production.loqed.com/api) and a decoder for the
// webhooks the LOQED cloud sends.
//
// # Authentication
//
// [New] takes a personal access token, sent as a bearer token. Create one at
// https://integrations.loqed.com/personal-access-tokens, or with
// [github.com/t3hk0d3/go-loqed/cloud/portal] from an email and password.
// A rejected or expired token gives an error wrapping loqed.ErrUnauthorized.
//
// # Rate limit
//
// LOQED blocks an account for 12 hours after more than 12 status reads in
// 12 hours, counted per account across all its tokens. [Client.ListLocks]
// is such a read: call it at startup or after a failure, cache the result,
// and never poll it. Cloud commands do not count towards this limit.
//
// # Locks and bridge credentials
//
// [Client.ListLocks] returns each lock with its cloud state and, when the
// lock has a bridge, the bridge's IP address and keys
// ([Lock.HasLocalCredentials]). Pass them to the bridge package to talk to
// the lock locally. The keys are [loqed.Secret] values: printing or logging
// a Lock shows them as "[redacted]".
//
// # Commands
//
// [Client.Command] asks the cloud to move the bolt, using the lock key that
// LOQED created for the token. A nil error means only that the cloud
// accepted the request; the lock's webhooks (bridge or cloud) confirm the
// move. Send a command again only after an error wrapping
// loqed.ErrUnreachable, never after loqed.ErrNoResponse. [ErrKeyDeleted]
// means that key was deleted in the LOQED app; reads still work.
//
// # Cloud webhooks
//
// [ParseWebhook] decodes the body of a webhook configured in the LOQED
// portal. Cloud webhooks are not signed: authenticate the request yourself,
// for example with a long random secret in the URL path. LockID in a cloud
// webhook is the cloud's numeric internal id, not [Lock.ID]. Fields that
// carry the account e-mail or name are never decoded.
//
// # HTTP client
//
// The default HTTP client has a 15 s timeout, opens a fresh connection per
// request and does not follow redirects (the API redirects unauthenticated
// calls to an HTML login page). A client passed to [WithHTTPClient] must
// keep both: DisableKeepAlives on its transport, so net/http never silently
// resends a command on a reused connection, and a CheckRedirect that
// returns http.ErrUseLastResponse.
//
// # Concurrency
//
// A [Client] is not changed after [New] and is safe for concurrent use by
// multiple goroutines. [ParseWebhook] is a pure function.
package cloud

// Package portal drives the LOQED Integrations portal (the undocumented
// "Management API"): log in with an email and password and manage the
// account's personal access tokens, which the cloud package uses.
//
// The portal is a Laravel + Inertia web app without a public API, so this
// package behaves like a browser: [Client.Login] starts a [Session] with
// its own cookie jar and CSRF token. Wrong credentials give an error
// wrapping loqed.ErrUnauthorized; a change on LOQED's side that the
// package does not understand gives loqed.ErrInvalidPayload. Errors never
// contain the password, cookies, CSRF tokens or portal HTML.
//
// [Session.CreateToken] returns the new token's value; the portal shows it
// only once, so store it. The create request is sent exactly once: after an
// error wrapping loqed.ErrNoResponse the token may exist anyway, so check
// with [Session.ListTokens] before creating another. Tokens expire (LOQED
// issues them for about six months); revoking one with
// [Session.RevokeToken] does not delete the lock key the cloud created for
// it.
//
// # Concurrency
//
// A [Client] holds only its configuration and is safe for concurrent use.
// A [Session] is not: use each session from one goroutine at a time, and
// call [Session.Logout] when done.
package portal

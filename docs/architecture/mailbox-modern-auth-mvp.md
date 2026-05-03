# Mailbox Modern Auth MVP

### Goal

This document describes the first mailbox-account modern-authentication MVP for `LPE`.

The goal is to extend modern authentication to mailbox accounts without breaking the current protocol adapters.

### Scope

The MVP now covers:

- local mailbox password login for webmail and session-based APIs
- optional mailbox `OIDC` login through confidential authorization-code flow
- optional user `TOTP` enrollment for interactive password login
- mailbox `OAuth2` bearer access-token minting from an authenticated mailbox session
- mailbox app passwords for legacy protocol clients that still use basic credentials
- audit logging of mailbox authentication outcomes and methods

### Non-goals for this MVP

The MVP intentionally does not implement:

- public mailbox `OAuth2` authorization-code flow served directly by `LPE`
- refresh tokens
- passwordless-only mailbox mode
- recovery codes
- step-up policies
- mandatory `TOTP` enforcement for every basic-auth protocol request

### Architectural rules

- mailbox modern authentication must remain separate from administrator authentication
- mailbox `OIDC` configuration is distinct from administrator `OIDC` configuration because callbacks and client registrations may differ
- mailbox sessions stay internal to `LPE`
- mailbox authorization remains internal to `LPE`; the external `IdP` authenticates identity only
- every protocol adapter must continue to converge on the canonical mailbox and submission model
- enabling modern auth must not move internet-facing `SMTP` back into the core `LPE` service
- mailbox `OAuth2` access tokens are short-lived signed bearer tokens, not long-lived password substitutes

### Mailbox `OIDC` MVP

The mailbox `OIDC` flow follows the same bounded model as the admin MVP:

1. the user opens the webmail login page
2. the frontend asks the API for the mailbox authorization URL
3. `LPE` signs a short-lived `state` bound to the public callback URL
4. the user authenticates with the external `IdP`
5. the callback exchanges the code for a bearer token
6. `LPE` reads mailbox claims from `userinfo`
7. the external identity is matched to an existing mailbox account
8. `LPE` creates its own internal mailbox session

The MVP rules are:

- no mailbox account is auto-provisioned from the `IdP`
- existing mailbox accounts may be auto-linked by email when the setting is enabled
- the callback must match the real published origin
- `OIDC` secrets stay server-side

### User `TOTP` MVP

The mailbox `TOTP` MVP is intentionally limited to interactive password login:

- the authenticated user can enroll a `TOTP` factor
- the factor must be verified before activation
- when an active factor exists, `/api/mail/auth/login` requires a valid `TOTP` code
- factor revocation is supported

For this MVP, `TOTP` is not layered on top of the mailbox `OIDC` flow.

### Mailbox `OAuth2` bearer tokens

The mailbox `OAuth2` MVP is intentionally bounded:

- `LPE` does not yet expose a full public OAuth authorization server for mailbox clients
- an already authenticated mailbox session may mint a short-lived bearer access token through `/api/mail/auth/oauth/access-token`
- the token is signed server-side, scope-limited, and validated by mailbox protocol adapters without introducing a second account store
- the default scope covers `mail`, `imap`, `dav`, `activesync`, `ews`, `managesieve`, and `smtp`
- the default lifetime is one hour and is controlled through `LPE_MAIL_OAUTH_ACCESS_TOKEN_SECONDS`

This is sufficient to reduce the modern-auth gap for native and compatibility clients while keeping the v1 design small and internally consistent.

### App passwords

Mailbox app passwords exist to keep legacy protocols usable while mailbox MFA is introduced.

The MVP rules are:

- app passwords are per mailbox account
- the plaintext secret is shown only at creation time
- only the hash is stored
- use is tracked by `last_used_at`
- revocation is immediate

In this MVP, app passwords are accepted by current basic-auth protocol surfaces such as `IMAP`, `DAV`, `ActiveSync`, and `ManageSieve`.

### Audit model

Mailbox authentication must append audit events with a clear method label.

The initial method labels are:

- `password`
- `password+totp`
- `oidc`
- `oauth-bearer`
- `app-password`

Audit entries must also record failed interactive password and factor verification attempts.

### Protocol impact

This MVP keeps the existing protocol matrix stable:

- webmail and other bearer-session mailbox APIs can use password or mailbox `OIDC`
- mailbox APIs can also use short-lived mailbox `OAuth2` bearer access tokens when the token scope includes `mail`
- `JMAP` session and API endpoints continue to use the internal mailbox bearer session
- `IMAP` and `ManageSieve` now support `XOAUTH2` with the mailbox bearer access token
- `DAV` and `ActiveSync` accept the same mailbox bearer access token through `Authorization: Bearer`
- `IMAP`, `DAV`, `ActiveSync`, and `ManageSieve` remain compatible with existing password login
- the same legacy protocols may also use mailbox app passwords

That preserves compatibility while introducing a cleaner path toward stronger mailbox authentication.

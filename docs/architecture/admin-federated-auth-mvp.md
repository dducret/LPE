# Admin Federated Auth MVP

### Goal

This document describes the current modern admin-authentication MVP for `LPE`.

The scope remains limited to the core `LPE` administration plane.

### MVP goals

- support a robust local admin login
- support a better integrated admin `OIDC` login
- keep authorization strictly internal to `LPE`
- preserve structured admin roles and permissions
- provide a first usable admin `TOTP` flow for password login
- audit admin sign-ins and factor lifecycle actions

### Out of scope for the MVP

- federation for end-user mailbox accounts
- automatic administrator provisioning from the `IdP`
- group or role synchronization from the `IdP`
- advanced local `ID Token` validation
- mandatory `PKCE`
- broad mandatory `MFA` enforcement for all admins
- recovery codes
- `TOTP` layered on top of `OIDC`

### `OIDC` flow

The `OIDC` flow remains a confidential authorization code flow:

1. the administrator opens the console
2. the frontend asks the API for the authorization URL
3. the API signs a `state` with the expected callback and timestamp
4. `LPE` uses configured endpoints or resolves `/.well-known/openid-configuration` from the issuer when only issuer metadata and secrets are provided
5. the user authenticates with the `IdP`
6. the `IdP` redirects back to the `LPE` callback
7. `LPE` exchanges the `code` for an access token
8. `LPE` fetches claims from `userinfo`
9. `LPE` binds the federated identity to an existing administrator
10. `LPE` creates an internal admin session with auth method `oidc`

### Admin password flow

The admin password flow is now:

1. the administrator submits email and password
2. `LPE` verifies the local `argon2` credential
3. when an active admin `TOTP` factor exists, a valid `TOTP` code is required
4. `LPE` creates an internal admin session with auth method `password` or `password+totp`
5. `LPE` records success and failure in the internal audit log

### Authorization rules

The `OIDC` provider authenticates the identity. Authorization remains entirely inside `LPE`.

The MVP rules are:

- a federated login must match an existing `LPE` administrator
- when auto-link is disabled, only an already registered `issuer + subject` mapping is accepted
- when auto-link is enabled, `LPE` may automatically bind a federated identity to an already created administrator with the same email address
- no administrator is automatically created from the `IdP`
- `LPE` roles and permissions remain the source of truth

### Admin `TOTP` MVP

The MVP now implements:

- admin `TOTP` enrollment through the API
- initial factor verification before activation
- `TOTP` verification during password login when an active factor exists
- factor revocation
- audit logging for enrollment, verification, revocation, and sign-in events

### Current limits

- no recovery codes
- no step-up policies
- no `TOTP` enforcement on the `OIDC` flow
- the MVP stores the `TOTP` secret as-is in the database and treats it as sensitive data

### Security constraints

- local admin password login remains supported for bootstrap and recovery
- bootstrap may use `admin@example.test` with `ChangeMeNow$` for the first operational sign-in
- that bootstrap secret must be changed immediately in real deployments
- the `OIDC` callback must match the real public origin
- `OIDC` secrets remain server-side
- the current `OIDC` flow still relies on `userinfo` for this MVP

### Licensing and dependencies

The MVP does not introduce an additional `OIDC` library.

The implementation reuses dependencies already present in the workspace. `TOTP` is computed without introducing a new federation dependency.



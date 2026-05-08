# Admin Federated Authentication

## Current State/Functionality Overview

The administration plane supports local password login, optional `OIDC` confidential-code login, role mapping, and password-login `TOTP`. Authorization remains inside `LPE`.

## Implementation/Usage

- `OIDC` flow:
  - redirects an admin to the configured identity provider
  - validates the callback state
  - exchanges the code for tokens
  - loads identity through configured endpoints or `/.well-known/openid-configuration`
  - maps the identity to an existing administrator
  - creates an `LPE` admin session
- Password flow:
  - verifies the local password hash
  - requires `TOTP` when an active admin factor exists
  - records the authentication method on the session
- Authorization:
  - uses `LPE` admin records, roles, and normalized permissions
  - does not trust identity-provider group membership as authorization
- `TOTP`:
  - supports enrollment
  - verifies the first code before activation
  - supports activation, verification, and removal
  - treats the stored secret as sensitive data
- Security constraints:
  - local password login remains available for bootstrap, recovery, and fallback
  - session cookies must remain HTTP-only and secure in production
  - `OIDC` relies on `userinfo`
  - no additional `OIDC` library is introduced

## Reference Table/List

| Area | Current fact |
| --- | --- |
| Admin local auth | supported |
| Admin `OIDC` | confidential authorization-code flow |
| Admin `TOTP` | password-login second factor |
| Authorization source | `LPE` roles and permissions |
| IdP authorization | not authoritative |
| Dependency policy | follows `LICENSE.md` |

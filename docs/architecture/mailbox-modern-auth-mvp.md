# Mailbox Modern Authentication

## Current State/Functionality Overview

Mailbox authentication supports local password login, optional mailbox `OIDC`, optional user `TOTP`, short-lived bearer access tokens, and app passwords for non-interactive protocol clients. All protocol adapters must map authentication to the same mailbox account identity.

## Implementation/Usage

- Local interactive login:
  - endpoint: `/api/mail/auth/login`
  - verifies `account_credentials`
  - requires `TOTP` when a verified factor exists
  - creates an `account_sessions` session
- OAuth-style token issuance:
  - endpoint: `/api/mail/auth/oauth/access-token`
  - issues mailbox bearer access tokens
  - scopes tokens to a mailbox account
- Mailbox `OIDC`:
  - ends in the same internal mailbox session model
  - does not replace canonical account authorization
- User `TOTP`:
  - supports enrollment and verification
  - protects interactive mailbox login
- App passwords:
  - support protocols that cannot use interactive login
  - must be revocable
  - must be auditable
- Protocol impact:
  - `JMAP`, `DAV`, `ActiveSync`, `EWS`, and `IMAP` must resolve to the canonical account
  - no protocol-specific identity store

## Reference Table/List

| Mechanism | Use |
| --- | --- |
| local password | interactive mailbox login |
| mailbox `OIDC` | interactive federated login |
| `TOTP` | second factor for interactive login |
| bearer token | API/protocol bearer authentication |
| app password | non-interactive client authentication |

| Endpoint | Purpose |
| --- | --- |
| `/api/mail/auth/login` | mailbox session login |
| `/api/mail/auth/oauth/access-token` | mailbox bearer token issuance |

# Tenancy, Identity, and Administration

## Current State/Functionality Overview

`LPE` is multi-tenant: global administrators manage the platform, and tenant administrators manage tenant domains and mailboxes. Identity and authorization are canonical `LPE` concerns.

## Implementation/Usage

- Tenancy:
  - tenants own domains
  - tenant administrators manage their tenant domains and mailboxes
  - global administrators manage cross-tenant platform state
  - runtime queries must enforce tenant/domain/account boundaries
  - attachment deduplication is domain-scoped
- Administration identity:
  - local admin password login
  - optional admin `OIDC`
  - admin `TOTP` for password login
  - roles paired with normalized permissions
- Mailbox identity:
  - local mailbox password login
  - optional mailbox `OIDC`
  - optional mailbox `TOTP`
  - bearer access tokens
  - app passwords for non-interactive protocol clients
- Authorization:
  - IdP authentication does not grant platform authorization by itself
  - `LPE` roles and permissions remain authoritative
  - protocol adapters must resolve to canonical account identities
- Administration UI:
  - use full-width management lists
  - put primary `New` or `Create` action in the list header
  - use a right-side drawer for creation, details, and contextual actions

## Reference Table/List

| Role | Scope |
| --- | --- |
| global administrator | platform-wide administration |
| tenant administrator | tenant domains and mailboxes |
| mailbox account | mailbox and collaboration data |

| Auth area | Mechanisms |
| --- | --- |
| admin | password, optional `OIDC`, password-login `TOTP` |
| mailbox | password, optional `OIDC`, optional `TOTP`, bearer tokens, app passwords |

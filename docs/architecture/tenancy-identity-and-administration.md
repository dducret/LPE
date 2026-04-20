# Tenancy, Identity, and Administration

### Goal

This document describes the multi-tenant model, identity, and administration roles.

### Multi-tenancy

`LPE` is multi-tenant.

Each tenant manages its own domain and the mailboxes of that domain.

Only `LPE-CT` in the `DMZ` is shared across domains.

### Runtime invariants

Runtime multi-tenancy follows these invariants:

- the runtime must never fall back to an implicit `"default"` tenant for mailbox, message, contact, calendar, task, blob, session, or queue access
- the tenant is resolved from the concrete runtime object:
  account logins and account sessions resolve the tenant from the mailbox domain
  mailbox, message, draft, attachment, and projection reads resolve the tenant from the owning account or stored row
  domain-scoped administrators resolve their tenant from the managed domain
- the control-plane global scope is explicit and separate from tenant data; it is not reused for mailbox runtime isolation
- account and administrator credentials are unique per `(tenant_id, email)`, not globally by email alone
- account and administrator sessions are bound to the same tenant as the credential that created them
- every storage query that reads or mutates tenant-owned runtime data must filter by both the resolved `tenant_id` and the object owner or identifier
- every sharing, delegation, or ACL resolution on contacts and calendars must verify `tenant_id`, `owner_account_id`, and `grantee_account_id` together; no cross-tenant grant is allowed
- inbound delivery must resolve the tenant independently for each accepted recipient so one SMTP transaction cannot collapse multiple tenants into one runtime scope
- attachment deduplication remains domain-scoped and therefore tenant-scoped in the current one-domain-per-tenant runtime model
- `Sieve` scripts, their active state, and the minimal `vacation` memory are scoped by `(tenant_id, account_id)` and must never be resolved outside the authenticated account or inbound-delivery recipient

### Modern identity

For a modern multi-tenant platform, native `OAuth2` and `OIDC` support is required.

For the administration plane, `LPE` now supports a first federated-login MVP based on confidential `OIDC` code flow, while keeping local password login available for bootstrap, recovery, and fallback.

The initial scope stays intentionally limited:

- federated login currently applies to the administration back office
- mailbox-account login remains password-based in v1
- `ManageSieve` reuses the same mailbox-account login and does not introduce a separate identity surface
- no passwordless-only mode is required in v1
- administrator factors now support a first `TOTP` flow for password login

### Federated admin MVP

This first building block follows these rules:

- local password login stays available unless an administrator explicitly disables it
- `OIDC` login must resolve to an existing `LPE` administrator identity
- no administrator is automatically created from the `IdP` in v1
- optional email auto-link may bind a federated identity to an already existing administrator with the same address
- provider configuration is global to the core `LPE` administration plane
- the callback must match the real public origin exposed by the reverse proxy

In this MVP, the `IdP` authenticates the identity, but authorization stays inside `LPE`.

### Roles

- server administrator
- domain administrator
- transport operator
- compliance / audit role
- support / helpdesk
- end user

Roles are now paired with structured normalized permissions.

Built-in roles provide default permissions, while explicit permissions may be added for exceptional delegations.

Password login and `OIDC` login both resolve to the same internal role and permission model.

### Admin MFA MVP

The authentication model now records the authentication method used by each admin session and now supports a first administrator-factor lifecycle.

The current scope covers:

- admin `TOTP` enrollment through the API
- factor verification before activation
- `TOTP` verification during password login when an active factor exists
- factor revocation
- audit logging for admin sign-ins and factor actions

Still out of scope:

- recovery codes
- step-up policies
- `TOTP` layered on the current `OIDC` flow

### Administration pattern

The default administration pattern is:

- full-width list
- primary `New` or `Create` action
- right-side drawer for creation, details, and contextual actions

Those drawers must be deep-linkable.



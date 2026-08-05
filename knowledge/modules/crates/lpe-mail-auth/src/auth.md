---
type: Rust Module
title: auth
resource: crates/lpe-mail-auth/src/auth.rs#L1-L178
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-bail-result
  - external/argon2-password-hash-passwordhash-passwordverifier-argon2
  - external/axum-http-headermap
  - external/lpe-domain-normalization
  - external/lpe-storage-auditentryinput
  - external/crate-oauth-basic-credentials-bearer-token-decode-oauth-access-token-scope-allows-surface-accountprincipal-store-accountauthstore
  member_of:
  - packages/crates/lpe-mail-auth
---

# Contains

- [authenticate_account](../../../../functions/crates/lpe-mail-auth/src/auth/authenticate_account.md)
- [authenticate_bearer_access_token](../../../../functions/crates/lpe-mail-auth/src/auth/authenticate_bearer_access_token.md)
- [authenticate_plain_credentials](../../../../functions/crates/lpe-mail-auth/src/auth/authenticate_plain_credentials.md)
- [normalize_login_name](../../../../functions/crates/lpe-mail-auth/src/auth/normalize_login_name.md)
- [verify_password](../../../../functions/crates/lpe-mail-auth/src/auth/verify_password.md)

# Imports

- `anyhow::{anyhow, bail, Result}`
- `argon2::{
    password_hash::{PasswordHash, PasswordVerifier},
    Argon2,
}`
- `axum::http::HeaderMap`
- `lpe_domain::normalization`
- `lpe_storage::AuditEntryInput`
- `crate::{
    oauth::{
        basic_credentials, bearer_token, decode_oauth_access_token, scope_allows_surface,
        AccountPrincipal,
    },
    store::AccountAuthStore,
}`

# Member of

- [lpe-mail-auth](../../../../packages/crates/lpe-mail-auth.md)
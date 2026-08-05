---
type: Rust Module
title: src
resource: crates/lpe-mail-auth/src/lib.rs#L1-L17
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/pub-use-crate-auth-authenticate-account-authenticate-bearer-access-token-authenticate-plain-credentials-normalize-login-name-verify-password
  - external/pub-use-crate-oauth-basic-credentials-bearer-token-issue-oauth-access-token-normalize-scope-oauth-signing-secret-unix-time-accountprincipal-default-oauth-access-scope-default-oauth-access-token-seconds
  - external/pub-use-crate-store-accountauthstore-storefuture
  member_of:
  - packages/crates/lpe-mail-auth
---

# Imports

- `pub use crate::auth::{
    authenticate_account, authenticate_bearer_access_token, authenticate_plain_credentials,
    normalize_login_name, verify_password,
}`
- `pub use crate::oauth::{
    basic_credentials, bearer_token, issue_oauth_access_token, normalize_scope,
    oauth_signing_secret, unix_time, AccountPrincipal, DEFAULT_OAUTH_ACCESS_SCOPE,
    DEFAULT_OAUTH_ACCESS_TOKEN_SECONDS,
}`
- `pub use crate::store::{AccountAuthStore, StoreFuture}`

# Member of

- [lpe-mail-auth](../../../packages/crates/lpe-mail-auth.md)
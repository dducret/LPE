---
type: Rust Module
title: oauth
resource: crates/lpe-mail-auth/src/oauth.rs#L1-L224
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-bail-result
  - external/axum-http-headermap
  - external/base64-engine-general-purpose-standard-as-base64-url-safe-no-pad-engine-as
  - external/hmac-hmac-mac
  - external/serde-deserialize-serialize
  - external/sha2-sha256
  - external/std-env-time-systemtime-unix-epoch
  - external/uuid-uuid
  - external/crate-auth-normalize-login-name
  member_of:
  - packages/crates/lpe-mail-auth
---

# Contains

- [AccountPrincipal](../../../../classes/crates/lpe-mail-auth/src/oauth/AccountPrincipal.md)
- [OAuthAccessTokenClaims](../../../../classes/crates/lpe-mail-auth/src/oauth/OAuthAccessTokenClaims.md)
- [issue_oauth_access_token](../../../../functions/crates/lpe-mail-auth/src/oauth/issue_oauth_access_token.md)
- [bearer_token](../../../../functions/crates/lpe-mail-auth/src/oauth/bearer_token.md)
- [basic_credentials](../../../../functions/crates/lpe-mail-auth/src/oauth/basic_credentials.md)
- [normalize_scope](../../../../functions/crates/lpe-mail-auth/src/oauth/normalize_scope.md)
- [oauth_signing_secret](../../../../functions/crates/lpe-mail-auth/src/oauth/oauth_signing_secret.md)
- [decode_oauth_access_token](../../../../functions/crates/lpe-mail-auth/src/oauth/decode_oauth_access_token.md)
- [AccountPrincipalClaims](../../../../classes/crates/lpe-mail-auth/src/oauth/AccountPrincipalClaims.md)
- [encode_oauth_access_token](../../../../functions/crates/lpe-mail-auth/src/oauth/encode_oauth_access_token.md)
- [sign_payload](../../../../functions/crates/lpe-mail-auth/src/oauth/sign_payload.md)
- [verify_signature](../../../../functions/crates/lpe-mail-auth/src/oauth/verify_signature.md)
- [scope_allows_surface](../../../../functions/crates/lpe-mail-auth/src/oauth/scope_allows_surface.md)
- [unix_time](../../../../functions/crates/lpe-mail-auth/src/oauth/unix_time.md)
- [is_known_weak_secret](../../../../functions/crates/lpe-mail-auth/src/oauth/is_known_weak_secret.md)

# Imports

- `anyhow::{anyhow, bail, Result}`
- `axum::http::HeaderMap`
- `base64::{
    engine::general_purpose::{STANDARD as BASE64, URL_SAFE_NO_PAD},
    Engine as _,
}`
- `hmac::{Hmac, Mac}`
- `serde::{Deserialize, Serialize}`
- `sha2::Sha256`
- `std::{
    env,
    time::{SystemTime, UNIX_EPOCH},
}`
- `uuid::Uuid`
- `crate::auth::normalize_login_name`

# Member of

- [lpe-mail-auth](../../../../packages/crates/lpe-mail-auth.md)
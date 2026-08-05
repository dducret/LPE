---
type: Rust Module
title: account_oidc
resource: crates/lpe-admin-api/src/account_oidc.rs#L1-L262
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-bail-context-result
  - external/base64-engine-general-purpose-url-safe-no-pad-engine-as
  - external/lpe-storage-accountoidcclaims-securitysettings
  - external/reqwest-client-url
  - external/serde-deserialize-serialize
  - external/serde-json-value
  - external/sha2-digest-sha256
  - external/std-time-systemtime-unix-epoch
  member_of:
  - packages/crates/lpe-admin-api
---

# Contains

- [OidcStatePayload](../../../../classes/crates/lpe-admin-api/src/account_oidc/OidcStatePayload.md)
- [OidcTokenResponse](../../../../classes/crates/lpe-admin-api/src/account_oidc/OidcTokenResponse.md)
- [OidcDiscoveryDocument](../../../../classes/crates/lpe-admin-api/src/account_oidc/OidcDiscoveryDocument.md)
- [OidcResolvedEndpoints](../../../../classes/crates/lpe-admin-api/src/account_oidc/OidcResolvedEndpoints.md)
- [authorization_url](../../../../functions/crates/lpe-admin-api/src/account_oidc/authorization_url.md)
- [exchange_code_for_claims](../../../../functions/crates/lpe-admin-api/src/account_oidc/exchange_code_for_claims.md)
- [ensure_oidc_ready](../../../../functions/crates/lpe-admin-api/src/account_oidc/ensure_oidc_ready.md)
- [callback_url](../../../../functions/crates/lpe-admin-api/src/account_oidc/callback_url.md)
- [normalized_scopes](../../../../functions/crates/lpe-admin-api/src/account_oidc/normalized_scopes.md)
- [resolved_endpoints](../../../../functions/crates/lpe-admin-api/src/account_oidc/resolved_endpoints.md)
- [sign_state](../../../../functions/crates/lpe-admin-api/src/account_oidc/sign_state.md)
- [verify_state](../../../../functions/crates/lpe-admin-api/src/account_oidc/verify_state.md)
- [state_signature](../../../../functions/crates/lpe-admin-api/src/account_oidc/state_signature.md)
- [claim_string](../../../../functions/crates/lpe-admin-api/src/account_oidc/claim_string.md)
- [now_unix](../../../../functions/crates/lpe-admin-api/src/account_oidc/now_unix.md)

# Imports

- `anyhow::{anyhow, bail, Context, Result}`
- `base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _}`
- `lpe_storage::{AccountOidcClaims, SecuritySettings}`
- `reqwest::{Client, Url}`
- `serde::{Deserialize, Serialize}`
- `serde_json::Value`
- `sha2::{Digest, Sha256}`
- `std::time::{SystemTime, UNIX_EPOCH}`

# Member of

- [lpe-admin-api](../../../../packages/crates/lpe-admin-api.md)
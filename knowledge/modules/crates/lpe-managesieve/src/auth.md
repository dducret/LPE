---
type: Rust Module
title: auth
resource: crates/lpe-managesieve/src/auth.rs#L1-L84
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-bail-result
  - external/base64-engine-general-purpose-standard-as-base64-engine-as
  - external/lpe-mail-auth-authenticate-bearer-access-token-authenticate-plain-credentials
  - external/uuid-uuid
  - external/crate-parse-as-string-argument-store-managesievestore
  member_of:
  - packages/crates/lpe-managesieve
---

# Contains

- [AuthenticatedAccount](../../../../classes/crates/lpe-managesieve/src/auth/AuthenticatedAccount.md)
- [authenticate](../../../../functions/crates/lpe-managesieve/src/auth/authenticate.md)
- [require_auth](../../../../functions/crates/lpe-managesieve/src/auth/require_auth.md)
- [parse_xoauth2_initial_response](../../../../functions/crates/lpe-managesieve/src/auth/parse_xoauth2_initial_response.md)

# Imports

- `anyhow::{anyhow, bail, Result}`
- `base64::{engine::general_purpose::STANDARD as BASE64, Engine as _}`
- `lpe_mail_auth::{authenticate_bearer_access_token, authenticate_plain_credentials}`
- `uuid::Uuid`
- `crate::{
    parse::{as_string, Argument},
    store::ManageSieveStore,
}`

# Member of

- [lpe-managesieve](../../../../packages/crates/lpe-managesieve.md)
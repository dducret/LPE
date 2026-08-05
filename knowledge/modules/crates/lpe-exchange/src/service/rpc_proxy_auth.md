---
type: Rust Module
title: rpc_proxy_auth
resource: crates/lpe-exchange/src/service/rpc_proxy_auth.rs#L1-L47
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/axum-http-header-content-type-www-authenticate-headervalue-statuscode-response-intoresponse-response
  - external/lpe-mail-auth-accountprincipal
  - external/super-rpc-proxy-compat-status
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [rpc_proxy_accepted_response](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_auth/rpc_proxy_accepted_response.md)
- [rpc_proxy_auth_challenge_response](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_auth/rpc_proxy_auth_challenge_response.md)

# Imports

- `axum::{
    http::{
        header::{CONTENT_TYPE, WWW_AUTHENTICATE},
        HeaderValue, StatusCode,
    },
    response::{IntoResponse, Response},
}`
- `lpe_mail_auth::AccountPrincipal`
- `super::RPC_PROXY_COMPAT_STATUS`

# Member of

- [lpe-exchange](../../../../../packages/crates/lpe-exchange.md)
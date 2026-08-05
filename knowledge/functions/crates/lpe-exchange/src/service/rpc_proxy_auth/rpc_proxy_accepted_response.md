---
type: Rust Function
title: rpc_proxy_accepted_response
resource: crates/lpe-exchange/src/service/rpc_proxy_auth.rs#L12-L30
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/src/management_auth/ApiError/axum-response-intoresponse/into_response
  called_by:
  - functions/crates/lpe-exchange/src/service/mapi_http/ExchangeService/handle_rpc_proxy
---

# Signature

`pub(super) fn rpc_proxy_accepted_response(principal: &AccountPrincipal) -> Response`

# Calls

- [into_response](../../../../../../functions/LPE-CT/src/management_auth/ApiError/axum-response-intoresponse/into_response.md)

# Called by

- [handle_rpc_proxy](../../../../../../functions/crates/lpe-exchange/src/service/mapi_http/ExchangeService/handle_rpc_proxy.md)
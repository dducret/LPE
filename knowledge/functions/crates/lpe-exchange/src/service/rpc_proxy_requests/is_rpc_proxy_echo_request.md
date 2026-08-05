---
type: Rust Function
title: is_rpc_proxy_echo_request
resource: crates/lpe-exchange/src/service/rpc_proxy_requests.rs#L3-L10
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_requests/is_rpc_proxy_msrpc_request
  called_by:
  - functions/crates/lpe-exchange/src/service/mapi_http/ExchangeService/handle_rpc_proxy
---

# Signature

`pub(super) fn is_rpc_proxy_echo_request(method: &Method, headers: &HeaderMap) -> bool`

# Calls

- [is_rpc_proxy_msrpc_request](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_requests/is_rpc_proxy_msrpc_request.md)

# Called by

- [handle_rpc_proxy](../../../../../../functions/crates/lpe-exchange/src/service/mapi_http/ExchangeService/handle_rpc_proxy.md)
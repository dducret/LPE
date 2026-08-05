---
type: Rust Function
title: rpc_proxy_echo_response
resource: crates/lpe-exchange/src/service/rpc_proxy_stream.rs#L55-L57
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_binary_response
  called_by:
  - functions/crates/lpe-exchange/src/service/mapi_http/ExchangeService/handle_rpc_proxy
---

# Signature

`pub(super) fn rpc_proxy_echo_response() -> Response`

# Calls

- [rpc_proxy_binary_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_binary_response.md)

# Called by

- [handle_rpc_proxy](../../../../../../functions/crates/lpe-exchange/src/service/mapi_http/ExchangeService/handle_rpc_proxy.md)
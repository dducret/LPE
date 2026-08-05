---
type: Rust Function
title: rpc_proxy_mailstore_ping_response_for_connect
resource: crates/lpe-exchange/src/service/rpc_proxy_stream.rs#L44-L53
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_mailstore_held_open_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_rts/rpc_proxy_endpoint_connect_body
  called_by:
  - functions/crates/lpe-exchange/src/service/mapi_http/ExchangeService/handle_rpc_proxy
---

# Signature

`pub(super) fn rpc_proxy_mailstore_ping_response_for_connect( uri: &Uri, connect: RpcProxyOutDataConnect, ) -> Response`

# Calls

- [rpc_proxy_mailstore_held_open_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_mailstore_held_open_response.md)
- [rpc_proxy_endpoint_connect_body](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_rts/rpc_proxy_endpoint_connect_body.md)

# Called by

- [handle_rpc_proxy](../../../../../../functions/crates/lpe-exchange/src/service/mapi_http/ExchangeService/handle_rpc_proxy.md)
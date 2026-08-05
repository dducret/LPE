---
type: Rust Function
title: parse_rpc_proxy_out_data_connect_request
resource: crates/lpe-exchange/src/service/rpc_proxy_rts.rs#L19-L31
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_requests/is_rpc_proxy_msrpc_request
  - functions/crates/lpe-exchange/src/service/rpc_proxy_rts/parse_rpc_proxy_conn_a1_rts_pdu
  called_by:
  - functions/crates/lpe-exchange/src/service/mapi_http/ExchangeService/handle_rpc_proxy
---

# Signature

`pub(super) fn parse_rpc_proxy_out_data_connect_request( method: &Method, headers: &HeaderMap, request_body: &[u8], ) -> Option<RpcProxyOutDataConnect>`

# Calls

- [is_rpc_proxy_msrpc_request](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_requests/is_rpc_proxy_msrpc_request.md)
- [parse_rpc_proxy_conn_a1_rts_pdu](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_rts/parse_rpc_proxy_conn_a1_rts_pdu.md)

# Called by

- [handle_rpc_proxy](../../../../../../functions/crates/lpe-exchange/src/service/mapi_http/ExchangeService/handle_rpc_proxy.md)
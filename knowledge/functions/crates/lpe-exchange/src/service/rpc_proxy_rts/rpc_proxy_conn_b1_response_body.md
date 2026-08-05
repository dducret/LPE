---
type: Rust Function
title: rpc_proxy_conn_b1_response_body
resource: crates/lpe-exchange/src/service/rpc_proxy_rts.rs#L62-L68
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_rts/rpc_proxy_conn_b1_virtual_connection_cookie
  - functions/crates/lpe-exchange/src/service/rpc_proxy_rts/rpc_proxy_connection_established_pdu
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_in_channel_response_for_endpoint_query
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_in_channel_response_for_endpoint_query_with_store_response
---

# Signature

`pub(super) fn rpc_proxy_conn_b1_response_body(request: &[u8]) -> Option<RpcProxyInChannelResponse>`

# Calls

- [rpc_proxy_conn_b1_virtual_connection_cookie](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_rts/rpc_proxy_conn_b1_virtual_connection_cookie.md)
- [rpc_proxy_connection_established_pdu](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_rts/rpc_proxy_connection_established_pdu.md)

# Called by

- [rpc_proxy_in_channel_response_for_endpoint_query](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_in_channel_response_for_endpoint_query.md)
- [rpc_proxy_in_channel_response_for_endpoint_query_with_store_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_in_channel_response_for_endpoint_query_with_store_response.md)
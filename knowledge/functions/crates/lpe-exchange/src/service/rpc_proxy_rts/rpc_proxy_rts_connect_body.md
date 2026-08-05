---
type: Rust Function
title: rpc_proxy_rts_connect_body
resource: crates/lpe-exchange/src/service/rpc_proxy_rts.rs#L33-L38
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_rts/rpc_proxy_connection_timeout_pdu
  - functions/crates/lpe-exchange/src/service/rpc_proxy_rts/rpc_proxy_connection_established_pdu
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_rts_connect_response
---

# Signature

`pub(super) fn rpc_proxy_rts_connect_body(client_receive_window_size: u32) -> Vec<u8>`

# Calls

- [rpc_proxy_connection_timeout_pdu](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_rts/rpc_proxy_connection_timeout_pdu.md)
- [rpc_proxy_connection_established_pdu](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_rts/rpc_proxy_connection_established_pdu.md)

# Called by

- [rpc_proxy_rts_connect_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_rts_connect_response.md)
---
type: Rust Function
title: rpc_proxy_connection_established_pdu
resource: crates/lpe-exchange/src/service/rpc_proxy_rts.rs#L51-L60
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_rts/rpc_proxy_rts_header
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_rts/rpc_proxy_rts_connect_body
  - functions/crates/lpe-exchange/src/service/rpc_proxy_rts/rpc_proxy_conn_b1_response_body
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_mailstore_held_open_response
---

# Signature

`pub(super) fn rpc_proxy_connection_established_pdu(receive_window_size: u32) -> Vec<u8>`

# Calls

- [rpc_proxy_rts_header](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_rts/rpc_proxy_rts_header.md)

# Called by

- [rpc_proxy_rts_connect_body](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_rts/rpc_proxy_rts_connect_body.md)
- [rpc_proxy_conn_b1_response_body](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_rts/rpc_proxy_conn_b1_response_body.md)
- [rpc_proxy_mailstore_held_open_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_mailstore_held_open_response.md)
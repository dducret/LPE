---
type: Rust Function
title: rpc_proxy_endpoint_connect_body
resource: crates/lpe-exchange/src/service/rpc_proxy_rts.rs#L40-L42
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_rts/rpc_proxy_connection_timeout_pdu
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_mailstore_ping_response_for_connect
---

# Signature

`pub(super) fn rpc_proxy_endpoint_connect_body() -> Vec<u8>`

# Calls

- [rpc_proxy_connection_timeout_pdu](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_rts/rpc_proxy_connection_timeout_pdu.md)

# Called by

- [rpc_proxy_mailstore_ping_response_for_connect](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_mailstore_ping_response_for_connect.md)
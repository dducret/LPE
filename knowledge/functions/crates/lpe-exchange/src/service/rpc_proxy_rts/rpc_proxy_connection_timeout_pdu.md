---
type: Rust Function
title: rpc_proxy_connection_timeout_pdu
resource: crates/lpe-exchange/src/service/rpc_proxy_rts.rs#L44-L49
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_rts/rpc_proxy_rts_header
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_rts/rpc_proxy_rts_connect_body
  - functions/crates/lpe-exchange/src/service/rpc_proxy_rts/rpc_proxy_endpoint_connect_body
---

# Signature

`fn rpc_proxy_connection_timeout_pdu() -> Vec<u8>`

# Calls

- [rpc_proxy_rts_header](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_rts/rpc_proxy_rts_header.md)

# Called by

- [rpc_proxy_rts_connect_body](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_rts/rpc_proxy_rts_connect_body.md)
- [rpc_proxy_endpoint_connect_body](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_rts/rpc_proxy_endpoint_connect_body.md)
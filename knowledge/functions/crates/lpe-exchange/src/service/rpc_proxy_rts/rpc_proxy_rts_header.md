---
type: Rust Function
title: rpc_proxy_rts_header
resource: crates/lpe-exchange/src/service/rpc_proxy_rts.rs#L126-L135
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_rts/rpc_proxy_connection_timeout_pdu
  - functions/crates/lpe-exchange/src/service/rpc_proxy_rts/rpc_proxy_connection_established_pdu
---

# Signature

`fn rpc_proxy_rts_header(flags: u16, command_count: u16, fragment_length: u16) -> Vec<u8>`

# Called by

- [rpc_proxy_connection_timeout_pdu](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_rts/rpc_proxy_connection_timeout_pdu.md)
- [rpc_proxy_connection_established_pdu](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_rts/rpc_proxy_connection_established_pdu.md)
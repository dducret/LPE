---
type: Rust Function
title: parse_rpc_rts_u32_command
resource: crates/lpe-exchange/src/service/rpc_proxy_rts.rs#L137-L149
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_codec/read_le_u32
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_rts/parse_rpc_proxy_conn_a1_rts_pdu
  - functions/crates/lpe-exchange/src/service/rpc_proxy_rts/rpc_proxy_conn_b1_virtual_connection_cookie
---

# Signature

`pub(super) fn parse_rpc_rts_u32_command( body: &[u8], offset: &mut usize, expected_command: u32, ) -> Option<u32>`

# Calls

- [read_le_u32](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_codec/read_le_u32.md)

# Called by

- [parse_rpc_proxy_conn_a1_rts_pdu](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_rts/parse_rpc_proxy_conn_a1_rts_pdu.md)
- [rpc_proxy_conn_b1_virtual_connection_cookie](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_rts/rpc_proxy_conn_b1_virtual_connection_cookie.md)
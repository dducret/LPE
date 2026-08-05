---
type: Rust Function
title: parse_rpc_rts_cookie_command
resource: crates/lpe-exchange/src/service/rpc_proxy_rts.rs#L151-L165
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_codec/read_le_u32
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_rts/parse_rpc_proxy_conn_a1_rts_pdu
  - functions/crates/lpe-exchange/src/service/rpc_proxy_rts/rpc_proxy_conn_b1_virtual_connection_cookie
---

# Signature

`pub(super) fn parse_rpc_rts_cookie_command( body: &[u8], offset: &mut usize, expected_command: u32, ) -> Option<[u8; 16]>`

# Calls

- [read_le_u32](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_codec/read_le_u32.md)
- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [parse_rpc_proxy_conn_a1_rts_pdu](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_rts/parse_rpc_proxy_conn_a1_rts_pdu.md)
- [rpc_proxy_conn_b1_virtual_connection_cookie](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_rts/rpc_proxy_conn_b1_virtual_connection_cookie.md)
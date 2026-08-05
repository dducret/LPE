---
type: Rust Function
title: rpc_proxy_conn_b1_virtual_connection_cookie
resource: crates/lpe-exchange/src/service/rpc_proxy_rts.rs#L95-L124
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/service/rpc_proxy_rts/parse_rpc_rts_u32_command
  - functions/crates/lpe-exchange/src/service/rpc_proxy_rts/parse_rpc_rts_cookie_command
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_rts/rpc_proxy_conn_b1_response_body
---

# Signature

`fn rpc_proxy_conn_b1_virtual_connection_cookie(body: &[u8]) -> Option<[u8; 16]>`

# Calls

- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [parse_rpc_rts_u32_command](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_rts/parse_rpc_rts_u32_command.md)
- [parse_rpc_rts_cookie_command](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_rts/parse_rpc_rts_cookie_command.md)

# Called by

- [rpc_proxy_conn_b1_response_body](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_rts/rpc_proxy_conn_b1_response_body.md)
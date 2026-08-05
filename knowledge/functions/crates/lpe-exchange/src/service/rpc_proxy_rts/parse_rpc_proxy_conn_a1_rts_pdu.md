---
type: Rust Function
title: parse_rpc_proxy_conn_a1_rts_pdu
resource: crates/lpe-exchange/src/service/rpc_proxy_rts.rs#L70-L93
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/service/rpc_proxy_rts/parse_rpc_rts_u32_command
  - functions/crates/lpe-exchange/src/service/rpc_proxy_rts/parse_rpc_rts_cookie_command
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_rts/parse_rpc_proxy_out_data_connect_request
---

# Signature

`fn parse_rpc_proxy_conn_a1_rts_pdu(body: &[u8]) -> Option<RpcProxyOutDataConnect>`

# Calls

- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [parse_rpc_rts_u32_command](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_rts/parse_rpc_rts_u32_command.md)
- [parse_rpc_rts_cookie_command](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_rts/parse_rpc_rts_cookie_command.md)

# Called by

- [parse_rpc_proxy_out_data_connect_request](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_rts/parse_rpc_proxy_out_data_connect_request.md)
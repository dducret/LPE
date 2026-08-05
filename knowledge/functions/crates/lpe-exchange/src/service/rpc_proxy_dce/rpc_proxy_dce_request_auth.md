---
type: Rust Function
title: rpc_proxy_dce_request_auth
resource: crates/lpe-exchange/src/service/rpc_proxy_dce.rs#L331-L344
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_request_auth_trailer_offset
  - functions/crates/lpe-exchange/src/service/rpc_proxy_codec/read_le_u32
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_response_with_request_auth
---

# Signature

`fn rpc_proxy_dce_request_auth(request: &[u8]) -> Option<RpcProxyDceRequestAuth>`

# Calls

- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [rpc_proxy_dce_request_auth_trailer_offset](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_request_auth_trailer_offset.md)
- [read_le_u32](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_codec/read_le_u32.md)

# Called by

- [rpc_proxy_dce_response_with_request_auth](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_response_with_request_auth.md)
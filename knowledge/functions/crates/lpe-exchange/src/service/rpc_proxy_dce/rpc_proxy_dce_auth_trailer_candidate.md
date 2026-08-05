---
type: Rust Function
title: rpc_proxy_dce_auth_trailer_candidate
resource: crates/lpe-exchange/src/service/rpc_proxy_dce.rs#L307-L329
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_request_auth_trailer_offset
---

# Signature

`fn rpc_proxy_dce_auth_trailer_candidate( request: &[u8], offset: usize, auth_pad_length: usize, ) -> bool`

# Calls

- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [rpc_proxy_dce_request_auth_trailer_offset](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_request_auth_trailer_offset.md)
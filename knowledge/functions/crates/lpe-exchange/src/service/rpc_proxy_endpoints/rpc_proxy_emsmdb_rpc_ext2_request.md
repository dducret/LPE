---
type: Rust Function
title: rpc_proxy_emsmdb_rpc_ext2_request
resource: crates/lpe-exchange/src/service/rpc_proxy_endpoints.rs#L261-L304
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_emsmdb_rpc_ext2_response_for_principal
---

# Signature

`fn rpc_proxy_emsmdb_rpc_ext2_request(request: &[u8]) -> Result<([u8; 20], Vec<u8>)>`

# Calls

- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [rpc_proxy_emsmdb_rpc_ext2_response_for_principal](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_emsmdb_rpc_ext2_response_for_principal.md)
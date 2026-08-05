---
type: Rust Function
title: rpc_proxy_emsmdb_rpc_ext2_response_with_rop_buffer
resource: crates/lpe-exchange/src/service/rpc_proxy_endpoints.rs#L215-L236
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_rpc_header_ext_payload
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_push_emsmdb_context_handle
  - functions/crates/lpe-exchange/src/service/rpc_proxy_codec/push_le_u32
  - functions/crates/lpe-exchange/src/service/rpc_proxy_codec/rpc_proxy_push_ndr_byte_array
  - functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_response
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_emsmdb_rpc_ext2_response
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_emsmdb_rpc_ext2_response_for_principal
---

# Signature

`fn rpc_proxy_emsmdb_rpc_ext2_response_with_rop_buffer( call_id: u32, context: &[u8; 20], rop_buffer: Vec<u8>, ) -> Vec<u8>`

# Calls

- [rpc_proxy_rpc_header_ext_payload](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_rpc_header_ext_payload.md)
- [rpc_proxy_push_emsmdb_context_handle](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_push_emsmdb_context_handle.md)
- [push_le_u32](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_codec/push_le_u32.md)
- [rpc_proxy_push_ndr_byte_array](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_codec/rpc_proxy_push_ndr_byte_array.md)
- [rpc_proxy_dce_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_response.md)

# Called by

- [rpc_proxy_emsmdb_rpc_ext2_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_emsmdb_rpc_ext2_response.md)
- [rpc_proxy_emsmdb_rpc_ext2_response_for_principal](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_emsmdb_rpc_ext2_response_for_principal.md)
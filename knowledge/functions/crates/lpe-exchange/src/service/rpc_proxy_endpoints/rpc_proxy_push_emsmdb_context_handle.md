---
type: Rust Function
title: rpc_proxy_push_emsmdb_context_handle
resource: crates/lpe-exchange/src/service/rpc_proxy_endpoints.rs#L246-L248
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_emsmdb_connect_ex_response_with_context
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_emsmdb_rpc_ext2_response_with_rop_buffer
---

# Signature

`fn rpc_proxy_push_emsmdb_context_handle(stub: &mut Vec<u8>, context: &[u8; 20])`

# Called by

- [rpc_proxy_emsmdb_connect_ex_response_with_context](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_emsmdb_connect_ex_response_with_context.md)
- [rpc_proxy_emsmdb_rpc_ext2_response_with_rop_buffer](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_emsmdb_rpc_ext2_response_with_rop_buffer.md)
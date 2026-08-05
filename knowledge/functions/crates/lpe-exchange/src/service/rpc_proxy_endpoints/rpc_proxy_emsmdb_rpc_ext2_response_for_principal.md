---
type: Rust Function
title: rpc_proxy_emsmdb_rpc_ext2_response_for_principal
resource: crates/lpe-exchange/src/service/rpc_proxy_endpoints.rs#L172-L213
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_emsmdb_rpc_ext2_request
  - functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_fault_response
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/execute_rpc_emsmdb_rops
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_emsmdb_rpc_ext2_response_with_rop_buffer
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_endpoint_response_for_fragment_with_store
---

# Signature

`pub(super) async fn rpc_proxy_emsmdb_rpc_ext2_response_for_principal<S, V>( store: &S, validator: &Validator<V>, principal: &AccountPrincipal, call_id: u32, request: &[u8], ) -> Vec<u8> where S: ExchangeStore, V: Detector,`

# Calls

- [rpc_proxy_emsmdb_rpc_ext2_request](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_emsmdb_rpc_ext2_request.md)
- [rpc_proxy_dce_fault_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_fault_response.md)
- [execute_rpc_emsmdb_rops](../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/execute_rpc_emsmdb_rops.md)
- [rpc_proxy_emsmdb_rpc_ext2_response_with_rop_buffer](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_emsmdb_rpc_ext2_response_with_rop_buffer.md)

# Called by

- [rpc_proxy_endpoint_response_for_fragment_with_store](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_endpoint_response_for_fragment_with_store.md)
---
type: Rust Function
title: rpc_proxy_bound_dce_context_interface
resource: crates/lpe-exchange/src/service/rpc_proxy_dce.rs#L77-L87
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_bound_dce_contexts
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_endpoint_response_for_fragment
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_endpoint_response_for_fragment_with_store
---

# Signature

`pub(super) fn rpc_proxy_bound_dce_context_interface( endpoint_query: &str, context_id: u16, ) -> Option<RpcProxyDceBoundInterface>`

# Calls

- [rpc_proxy_bound_dce_contexts](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_bound_dce_contexts.md)
- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [rpc_proxy_endpoint_response_for_fragment](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_endpoint_response_for_fragment.md)
- [rpc_proxy_endpoint_response_for_fragment_with_store](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_endpoint_response_for_fragment_with_store.md)
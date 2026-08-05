---
type: Rust Function
title: rpc_proxy_remember_dce_bind_contexts
resource: crates/lpe-exchange/src/service/rpc_proxy_dce.rs#L89-L131
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_bind_context_count
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_interface_for_abstract_syntax
  - functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_bound_dce_contexts
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_endpoint_response_for_fragment
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_endpoint_response_for_fragment_with_store
---

# Signature

`pub(super) fn rpc_proxy_remember_dce_bind_contexts(endpoint_query: &str, request: &[u8])`

# Calls

- [rpc_proxy_dce_bind_context_count](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_bind_context_count.md)
- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [rpc_proxy_dce_interface_for_abstract_syntax](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_interface_for_abstract_syntax.md)
- [rpc_proxy_bound_dce_contexts](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_bound_dce_contexts.md)

# Called by

- [rpc_proxy_endpoint_response_for_fragment](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_endpoint_response_for_fragment.md)
- [rpc_proxy_endpoint_response_for_fragment_with_store](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_endpoint_response_for_fragment_with_store.md)
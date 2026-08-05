---
type: Rust Function
title: rpc_proxy_dce_alter_context_response_body
resource: crates/lpe-exchange/src/service/rpc_proxy_dce.rs#L67-L75
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_bind_context_results
  - functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_default_context_results
  - functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_bind_context_count
  - functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_context_ack_body
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_endpoint_response_for_fragment
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_endpoint_response_for_fragment_with_store
---

# Signature

`pub(super) fn rpc_proxy_dce_alter_context_response_body(call_id: u32, request: &[u8]) -> Vec<u8>`

# Calls

- [rpc_proxy_dce_bind_context_results](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_bind_context_results.md)
- [rpc_proxy_dce_default_context_results](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_default_context_results.md)
- [rpc_proxy_dce_bind_context_count](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_bind_context_count.md)
- [rpc_proxy_dce_context_ack_body](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_context_ack_body.md)

# Called by

- [rpc_proxy_endpoint_response_for_fragment](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_endpoint_response_for_fragment.md)
- [rpc_proxy_endpoint_response_for_fragment_with_store](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_endpoint_response_for_fragment_with_store.md)
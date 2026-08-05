---
type: Rust Function
title: rpc_proxy_dce_bind_ack_body_with_result_count
resource: crates/lpe-exchange/src/service/rpc_proxy_dce.rs#L51-L57
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_default_context_results
  - functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_bind_ack_body_with_results
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_mailstore_held_open_response
---

# Signature

`pub(super) fn rpc_proxy_dce_bind_ack_body_with_result_count( call_id: u32, result_count: u8, ) -> Vec<u8>`

# Calls

- [rpc_proxy_dce_default_context_results](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_default_context_results.md)
- [rpc_proxy_dce_bind_ack_body_with_results](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_bind_ack_body_with_results.md)

# Called by

- [rpc_proxy_mailstore_held_open_response](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_mailstore_held_open_response.md)
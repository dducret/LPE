---
type: Rust Function
title: rpc_proxy_dce_bind_ack_body_with_results
resource: crates/lpe-exchange/src/service/rpc_proxy_dce.rs#L59-L65
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_context_ack_body
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_bind_ack_body
  - functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_bind_ack_body_with_result_count
---

# Signature

`fn rpc_proxy_dce_bind_ack_body_with_results( call_id: u32, results: &[RpcProxyDceContextResult], ) -> Vec<u8>`

# Calls

- [rpc_proxy_dce_context_ack_body](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_context_ack_body.md)

# Called by

- [rpc_proxy_dce_bind_ack_body](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_bind_ack_body.md)
- [rpc_proxy_dce_bind_ack_body_with_result_count](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_bind_ack_body_with_result_count.md)
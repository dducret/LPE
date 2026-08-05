---
type: Rust Function
title: rpc_proxy_dce_default_context_results
resource: crates/lpe-exchange/src/service/rpc_proxy_dce.rs#L223-L233
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_context_accept_result
  - functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_context_provider_rejection_result
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_bind_ack_body
  - functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_bind_ack_body_with_result_count
  - functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_alter_context_response_body
---

# Signature

`fn rpc_proxy_dce_default_context_results(result_count: u8) -> Vec<RpcProxyDceContextResult>`

# Calls

- [rpc_proxy_dce_context_accept_result](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_context_accept_result.md)
- [rpc_proxy_dce_context_provider_rejection_result](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_context_provider_rejection_result.md)

# Called by

- [rpc_proxy_dce_bind_ack_body](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_bind_ack_body.md)
- [rpc_proxy_dce_bind_ack_body_with_result_count](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_bind_ack_body_with_result_count.md)
- [rpc_proxy_dce_alter_context_response_body](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_alter_context_response_body.md)
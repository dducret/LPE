---
type: Rust Function
title: rpc_proxy_dce_bind_context_count
resource: crates/lpe-exchange/src/service/rpc_proxy_dce.rs#L199-L202
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_bind_ack_body
  - functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_alter_context_response_body
  - functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_remember_dce_bind_contexts
  - functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_bind_context_results
---

# Signature

`fn rpc_proxy_dce_bind_context_count(request: &[u8]) -> Option<u8>`

# Calls

- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [rpc_proxy_dce_bind_ack_body](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_bind_ack_body.md)
- [rpc_proxy_dce_alter_context_response_body](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_alter_context_response_body.md)
- [rpc_proxy_remember_dce_bind_contexts](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_remember_dce_bind_contexts.md)
- [rpc_proxy_dce_bind_context_results](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_bind_context_results.md)
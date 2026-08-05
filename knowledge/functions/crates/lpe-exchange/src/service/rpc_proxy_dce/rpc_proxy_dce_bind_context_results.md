---
type: Rust Function
title: rpc_proxy_dce_bind_context_results
resource: crates/lpe-exchange/src/service/rpc_proxy_dce.rs#L235-L255
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_bind_context_count
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_context_provider_rejection_result
  - functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_context_accept_result
  - functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_is_bind_time_feature_negotiation_syntax
  - functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_bind_time_feature_negotiation_result
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_bind_ack_body
  - functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_alter_context_response_body
---

# Signature

`fn rpc_proxy_dce_bind_context_results(request: &[u8]) -> Option<Vec<RpcProxyDceContextResult>>`

# Calls

- [rpc_proxy_dce_bind_context_count](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_bind_context_count.md)
- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [rpc_proxy_dce_context_provider_rejection_result](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_context_provider_rejection_result.md)
- [rpc_proxy_dce_context_accept_result](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_context_accept_result.md)
- [rpc_proxy_is_bind_time_feature_negotiation_syntax](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_is_bind_time_feature_negotiation_syntax.md)
- [rpc_proxy_dce_bind_time_feature_negotiation_result](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_bind_time_feature_negotiation_result.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [rpc_proxy_dce_bind_ack_body](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_bind_ack_body.md)
- [rpc_proxy_dce_alter_context_response_body](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_alter_context_response_body.md)
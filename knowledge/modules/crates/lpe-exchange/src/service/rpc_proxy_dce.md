---
type: Rust Module
title: rpc_proxy_dce
resource: crates/lpe-exchange/src/service/rpc_proxy_dce.rs#L1-L395
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/std-collections-hashmap
  - external/std-sync-mutex-oncelock
  - external/super-rpc-proxy-codec-read-le-u32
  - external/crate-ntlm
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [RpcProxyDceBoundInterface](../../../../../classes/crates/lpe-exchange/src/service/rpc_proxy_dce/RpcProxyDceBoundInterface.md)
- [RpcProxyDceContextResult](../../../../../classes/crates/lpe-exchange/src/service/rpc_proxy_dce/RpcProxyDceContextResult.md)
- [RpcProxyDceRequestAuth](../../../../../classes/crates/lpe-exchange/src/service/rpc_proxy_dce/RpcProxyDceRequestAuth.md)
- [rpc_proxy_dce_bind_ack_body](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_bind_ack_body.md)
- [rpc_proxy_dce_bind_ack_body_with_result_count](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_bind_ack_body_with_result_count.md)
- [rpc_proxy_dce_bind_ack_body_with_results](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_bind_ack_body_with_results.md)
- [rpc_proxy_dce_alter_context_response_body](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_alter_context_response_body.md)
- [rpc_proxy_bound_dce_context_interface](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_bound_dce_context_interface.md)
- [rpc_proxy_remember_dce_bind_contexts](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_remember_dce_bind_contexts.md)
- [rpc_proxy_dce_fault_response](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_fault_response.md)
- [rpc_proxy_dce_response_with_request_auth](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_response_with_request_auth.md)
- [rpc_proxy_dce_response](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_response.md)
- [rpc_proxy_dce_bind_context_count](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_bind_context_count.md)
- [rpc_proxy_bound_dce_contexts](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_bound_dce_contexts.md)
- [rpc_proxy_dce_interface_for_abstract_syntax](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_interface_for_abstract_syntax.md)
- [rpc_proxy_dce_default_context_results](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_default_context_results.md)
- [rpc_proxy_dce_bind_context_results](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_bind_context_results.md)
- [rpc_proxy_dce_context_accept_result](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_context_accept_result.md)
- [rpc_proxy_dce_context_provider_rejection_result](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_context_provider_rejection_result.md)
- [rpc_proxy_dce_bind_time_feature_negotiation_result](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_bind_time_feature_negotiation_result.md)
- [rpc_proxy_is_bind_time_feature_negotiation_syntax](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_is_bind_time_feature_negotiation_syntax.md)
- [rpc_proxy_dce_request_auth_trailer_offset](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_request_auth_trailer_offset.md)
- [rpc_proxy_dce_auth_trailer_candidate](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_auth_trailer_candidate.md)
- [rpc_proxy_dce_request_auth](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_request_auth.md)
- [rpc_proxy_dce_context_ack_body](../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_context_ack_body.md)

# Imports

- `std::collections::HashMap`
- `std::sync::{Mutex, OnceLock}`
- `super::rpc_proxy_codec::read_le_u32`
- `crate::ntlm`

# Member of

- [lpe-exchange](../../../../../packages/crates/lpe-exchange.md)
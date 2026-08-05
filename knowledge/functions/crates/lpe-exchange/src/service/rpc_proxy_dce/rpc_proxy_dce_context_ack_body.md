---
type: Rust Function
title: rpc_proxy_dce_context_ack_body
resource: crates/lpe-exchange/src/service/rpc_proxy_dce.rs#L346-L395
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/ntlm/connect_level_challenge_verifier
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_bind_ack_body_with_results
  - functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_alter_context_response_body
---

# Signature

`fn rpc_proxy_dce_context_ack_body( call_id: u32, packet_type: u8, results: &[RpcProxyDceContextResult], ) -> Vec<u8>`

# Calls

- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [connect_level_challenge_verifier](../../../../../../functions/crates/lpe-exchange/src/ntlm/connect_level_challenge_verifier.md)

# Called by

- [rpc_proxy_dce_bind_ack_body_with_results](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_bind_ack_body_with_results.md)
- [rpc_proxy_dce_alter_context_response_body](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_alter_context_response_body.md)
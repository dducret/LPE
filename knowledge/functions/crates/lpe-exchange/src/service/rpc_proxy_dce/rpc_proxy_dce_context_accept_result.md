---
type: Rust Function
title: rpc_proxy_dce_context_accept_result
resource: crates/lpe-exchange/src/service/rpc_proxy_dce.rs#L257-L263
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_default_context_results
  - functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_bind_context_results
---

# Signature

`fn rpc_proxy_dce_context_accept_result(transfer_syntax: [u8; 20]) -> RpcProxyDceContextResult`

# Called by

- [rpc_proxy_dce_default_context_results](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_default_context_results.md)
- [rpc_proxy_dce_bind_context_results](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_dce_bind_context_results.md)
---
type: Rust Function
title: rpc_proxy_bound_dce_contexts
resource: crates/lpe-exchange/src/service/rpc_proxy_dce.rs#L204-L209
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_bound_dce_context_interface
  - functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_remember_dce_bind_contexts
---

# Signature

`fn rpc_proxy_bound_dce_contexts( ) -> &'static Mutex<HashMap<String, HashMap<u16, RpcProxyDceBoundInterface>>>`

# Called by

- [rpc_proxy_bound_dce_context_interface](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_bound_dce_context_interface.md)
- [rpc_proxy_remember_dce_bind_contexts](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_remember_dce_bind_contexts.md)
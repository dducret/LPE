---
type: Rust Function
title: rpc_proxy_dce_interface_for_abstract_syntax
resource: crates/lpe-exchange/src/service/rpc_proxy_dce.rs#L211-L221
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_remember_dce_bind_contexts
---

# Signature

`fn rpc_proxy_dce_interface_for_abstract_syntax( abstract_syntax: &[u8], ) -> Option<RpcProxyDceBoundInterface>`

# Called by

- [rpc_proxy_remember_dce_bind_contexts](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_dce/rpc_proxy_remember_dce_bind_contexts.md)
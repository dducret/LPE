---
type: Rust Function
title: rpc_proxy_emsmdb_connect_ex_response_for_principal
resource: crates/lpe-exchange/src/service/rpc_proxy_endpoints.rs#L128-L134
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/create_rpc_emsmdb_context
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_emsmdb_connect_ex_response_with_context
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_endpoint_response_for_fragment_with_store
---

# Signature

`pub(super) fn rpc_proxy_emsmdb_connect_ex_response_for_principal( call_id: u32, principal: &AccountPrincipal, ) -> Vec<u8>`

# Calls

- [create_rpc_emsmdb_context](../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/create_rpc_emsmdb_context.md)
- [rpc_proxy_emsmdb_connect_ex_response_with_context](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_emsmdb_connect_ex_response_with_context.md)

# Called by

- [rpc_proxy_endpoint_response_for_fragment_with_store](../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_stream/rpc_proxy_endpoint_response_for_fragment_with_store.md)
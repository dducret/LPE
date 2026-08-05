---
type: Rust Function
title: create_rpc_emsmdb_context
resource: crates/lpe-exchange/src/mapi/session/lifecycle.rs#L169-L175
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/create_session
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_emsmdb_connect_ex_response_for_principal
---

# Signature

`pub(crate) fn create_rpc_emsmdb_context(principal: &AccountPrincipal) -> [u8; 20]`

# Calls

- [create_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/create_session.md)

# Called by

- [rpc_proxy_emsmdb_connect_ex_response_for_principal](../../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_emsmdb_connect_ex_response_for_principal.md)
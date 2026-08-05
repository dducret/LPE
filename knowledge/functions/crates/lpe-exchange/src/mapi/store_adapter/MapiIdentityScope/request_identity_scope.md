---
type: Rust Method
title: request_identity_scope
resource: crates/lpe-exchange/src/mapi/store_adapter.rs#L32-L39
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_response
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/execute_rpc_emsmdb_rops
---

# Signature

`pub(in crate::mapi) fn request_identity_scope( &self, ) -> crate::mapi::identity::MapiRequestIdentityScope`

# Called by

- [execute_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_response.md)
- [execute_rpc_emsmdb_rops](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/execute_rpc_emsmdb_rops.md)
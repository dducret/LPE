---
type: Rust Function
title: with_current_mapi_request_identity_scope
resource: crates/lpe-exchange/src/mapi/identity.rs#L144-L149
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_response
  - functions/crates/lpe-exchange/src/mapi/identity/request_scope_keeps_special_folder_parent_identity_logical_and_durable
  - functions/crates/lpe-exchange/src/mapi/identity/owner_and_grantee_scopes_keep_hierarchy_folder_wire_ids_separate
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/execute_rpc_emsmdb_rops
---

# Signature

`pub(crate) async fn with_current_mapi_request_identity_scope<'a, T: Send + 'a>( scope: MapiRequestIdentityScope, future: std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>, ) -> T`

# Called by

- [execute_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_response.md)
- [request_scope_keeps_special_folder_parent_identity_logical_and_durable](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/request_scope_keeps_special_folder_parent_identity_logical_and_durable.md)
- [owner_and_grantee_scopes_keep_hierarchy_folder_wire_ids_separate](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/owner_and_grantee_scopes_keep_hierarchy_folder_wire_ids_separate.md)
- [execute_rpc_emsmdb_rops](../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/execute_rpc_emsmdb_rops.md)
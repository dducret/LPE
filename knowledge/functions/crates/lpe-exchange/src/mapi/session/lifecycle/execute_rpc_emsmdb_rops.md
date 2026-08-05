---
type: Rust Function
title: execute_rpc_emsmdb_rops
resource: crates/lpe-exchange/src/mapi/session/lifecycle.rs#L177-L263
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/rpc_context_session_id
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/get_session
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/session_matches
  - functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_identity_scope
  - functions/crates/lpe-exchange/src/mapi/store_adapter/MapiIdentityScope/request_identity_scope
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/refresh_persisted_special_folder_aliases
  - functions/crates/lpe-exchange/src/mapi/identity/with_current_mapi_identity_codec
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/plan_mapi_store_access
  - functions/crates/lpe-exchange/src/mapi/identity/with_current_mapi_request_identity_scope
  - functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/mailboxes
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/emails
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/remove_session
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/store_session
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/identity_codec
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops
  called_by:
  - functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_emsmdb_rpc_ext2_response_for_principal
---

# Signature

`pub(crate) async fn execute_rpc_emsmdb_rops<S, V>( store: &S, validator: &Validator<V>, principal: &AccountPrincipal, context_handle: &[u8], rop_buffer: &[u8], ) -> Result<Vec<u8>> where S: ExchangeStore, V: Detector,`

# Calls

- [rpc_context_session_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/rpc_context_session_id.md)
- [get_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/get_session.md)
- [session_matches](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/session_matches.md)
- [load_mapi_identity_scope](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_identity_scope.md)
- [request_identity_scope](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/MapiIdentityScope/request_identity_scope.md)
- [refresh_persisted_special_folder_aliases](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/refresh_persisted_special_folder_aliases.md)
- [with_current_mapi_identity_codec](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/with_current_mapi_identity_codec.md)
- [plan_mapi_store_access](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/plan_mapi_store_access.md)
- [with_current_mapi_request_identity_scope](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/with_current_mapi_request_identity_scope.md)
- [load_mapi_store_for_access_plan](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan.md)
- [mailboxes](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/mailboxes.md)
- [emails](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/emails.md)
- [remove_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/remove_session.md)
- [store_session](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/store_session.md)
- [identity_codec](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/identity_codec.md)
- [execute_rops](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops.md)

# Called by

- [rpc_proxy_emsmdb_rpc_ext2_response_for_principal](../../../../../../../functions/crates/lpe-exchange/src/service/rpc_proxy_endpoints/rpc_proxy_emsmdb_rpc_ext2_response_for_principal.md)
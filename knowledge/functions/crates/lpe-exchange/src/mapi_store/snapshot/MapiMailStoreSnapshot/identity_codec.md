---
type: Rust Method
title: identity_codec
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L91-L93
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops
  - functions/crates/lpe-exchange/src/mapi/dispatch/logon/append_logon_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_hierarchy/append_synchronization_import_hierarchy_change_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_hierarchy/folder_version_for_snapshot
  - functions/crates/lpe-exchange/src/mapi/session/lifecycle/execute_rpc_emsmdb_rops
  - functions/crates/lpe-exchange/src/mapi/store_adapter/finalize_mapi_store_snapshot
---

# Signature

`pub(crate) fn identity_codec(&self) -> &crate::mapi::identity::MapiIdentityCodec`

# Called by

- [execute_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_response.md)
- [execute_rops](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops.md)
- [append_logon_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/logon/append_logon_response.md)
- [append_synchronization_import_hierarchy_change_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_hierarchy/append_synchronization_import_hierarchy_change_response.md)
- [folder_version_for_snapshot](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_hierarchy/folder_version_for_snapshot.md)
- [execute_rpc_emsmdb_rops](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/lifecycle/execute_rpc_emsmdb_rops.md)
- [finalize_mapi_store_snapshot](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/finalize_mapi_store_snapshot.md)
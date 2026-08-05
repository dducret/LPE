---
type: Rust Function
title: deleted_special_object_ids_for_folder
resource: crates/lpe-exchange/src/mapi/dispatch/sync_import.rs#L1246-L1331
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_object_ids_for_deleted_changes
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/mapi_object_ids_for_deleted_changes
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response
---

# Signature

`pub(super) async fn deleted_special_object_ids_for_folder<S>( store: &S, principal: &AccountPrincipal, folder_id: u64, snapshot: &MapiMailStoreSnapshot, changes: &MapiSyncChangeSet, ) -> Vec<u64> where S: ExchangeStore,`

# Calls

- [collaboration_folder_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id.md)
- [fetch_mapi_object_ids_for_deleted_changes](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/fetch_mapi_object_ids_for_deleted_changes.md)
- [mapi_object_ids_for_deleted_changes](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/mapi_object_ids_for_deleted_changes.md)

# Called by

- [append_synchronization_configure_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response.md)
---
type: Rust Function
title: changed_special_ids_for_folder
resource: crates/lpe-exchange/src/mapi/dispatch/sync_import.rs#L1187-L1244
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/calendar_content_sync_changed_ids_are_projected
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/calendar_content_sync_changed_ids_include_associated_config
---

# Signature

`pub(super) fn changed_special_ids_for_folder( folder_id: u64, snapshot: &MapiMailStoreSnapshot, changes: &MapiSyncChangeSet, ) -> Vec<Uuid>`

# Calls

- [collaboration_folder_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id.md)

# Called by

- [append_synchronization_configure_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response.md)
- [calendar_content_sync_changed_ids_are_projected](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/calendar_content_sync_changed_ids_are_projected.md)
- [calendar_content_sync_changed_ids_include_associated_config](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/calendar_content_sync_changed_ids_include_associated_config.md)
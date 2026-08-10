---
type: Rust Function
title: imported_event_transaction
resource: crates/lpe-exchange/src/mapi/dispatch/sync_import_message.rs#L42-L86
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_conflicts/sync_import_version_relation
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_conflicts/merge_sync_predecessor_change_lists
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_conflicts/imported_version_wins_last_writer
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_pending_event
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/append_synchronization_import_message_change_response
---

# Signature

`pub(super) fn imported_event_transaction( event: &crate::mapi_store::MapiEvent, mut imported_identity: MapiEventImportedIdentity, imported_last_modification_time: u64, fail_on_conflict: bool, ) -> Result<MapiEventTransaction, u32>`

# Calls

- [sync_import_version_relation](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_conflicts/sync_import_version_relation.md)
- [merge_sync_predecessor_change_lists](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_conflicts/merge_sync_predecessor_change_lists.md)
- [filetime_from_rfc3339_utc](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc.md)
- [imported_version_wins_last_writer](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_conflicts/imported_version_wins_last_writer.md)

# Called by

- [save_pending_event](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_pending_event.md)
- [append_synchronization_import_message_change_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/append_synchronization_import_message_change_response.md)
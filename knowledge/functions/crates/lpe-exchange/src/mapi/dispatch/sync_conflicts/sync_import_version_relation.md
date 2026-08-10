---
type: Rust Function
title: sync_import_version_relation
resource: crates/lpe-exchange/src/mapi/dispatch/sync_conflicts.rs#L12-L29
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_conflicts/predecessor_map_includes
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/imported_event_transaction
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/append_synchronization_import_message_change_response
---

# Signature

`pub(super) fn sync_import_version_relation( incoming_pcl: &[u8], current_pcl: &[u8], ) -> Result<SyncImportVersionRelation>`

# Calls

- [predecessor_map_includes](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_conflicts/predecessor_map_includes.md)

# Called by

- [imported_event_transaction](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/imported_event_transaction.md)
- [append_synchronization_import_message_change_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/append_synchronization_import_message_change_response.md)
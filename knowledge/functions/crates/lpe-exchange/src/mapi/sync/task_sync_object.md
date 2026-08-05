---
type: Rust Function
title: task_sync_object
resource: crates/lpe-exchange/src/mapi/sync.rs#L515-L561
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/task/task_property_value_with_reminder
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc
  - functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/task_size
  called_by:
  - functions/crates/lpe-exchange/src/mapi/sync/special_sync_objects_for
---

# Signature

`fn task_sync_object( task: &crate::mapi_store::MapiTask, reminder: Option<&lpe_storage::ClientReminder>, ) -> mapi_mailstore::SpecialMessageSyncFact`

# Calls

- [task_property_value_with_reminder](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/task/task_property_value_with_reminder.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [filetime_from_rfc3339_utc](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc.md)
- [task_size](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/task_size.md)

# Called by

- [special_sync_objects_for](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/special_sync_objects_for.md)
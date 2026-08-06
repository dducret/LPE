---
type: Rust Function
title: task_property_value_with_reminder
resource: crates/lpe-exchange/src/mapi/properties/task.rs#L12-L101
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/task/task_reminder_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id
  - functions/crates/lpe-exchange/src/mapi/properties/task/task_flag_status
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc
  - functions/crates/lpe-exchange/src/mapi/properties/task/task_percent_complete
  - functions/crates/lpe-exchange/src/mapi/properties/task/task_completion_date_filetime
  - functions/crates/lpe-exchange/src/mapi/properties/message/mapi_message_size_value
  - functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/task_size
  - functions/crates/lpe-exchange/src/mapi/properties/message/mapi_message_size_extended_value
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_uuid
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/task/task_property_value
  - functions/crates/lpe-exchange/src/mapi/sync/task_sync_object
  - functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_reminder_task_row
---

# Signature

`pub(in crate::mapi) fn task_property_value_with_reminder( task: &ClientTask, item_id: u64, folder_id: u64, property_tag: u32, reminder: Option<&lpe_storage::ClientReminder>, ) -> Option<MapiValue>`

# Calls

- [task_reminder_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/task/task_reminder_property_value.md)
- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [change_number_for_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id.md)
- [task_flag_status](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/task/task_flag_status.md)
- [filetime_from_rfc3339_utc](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc.md)
- [task_percent_complete](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/task/task_percent_complete.md)
- [task_completion_date_filetime](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/task/task_completion_date_filetime.md)
- [mapi_message_size_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/mapi_message_size_value.md)
- [task_size](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/task_size.md)
- [mapi_message_size_extended_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/mapi_message_size_extended_value.md)
- [source_key_for_uuid](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_uuid.md)
- [source_key_for_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id.md)

# Called by

- [task_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/task/task_property_value.md)
- [task_sync_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/task_sync_object.md)
- [serialize_reminder_task_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_reminder_task_row.md)
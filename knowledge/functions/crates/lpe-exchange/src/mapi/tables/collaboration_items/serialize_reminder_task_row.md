---
type: Rust Function
title: serialize_reminder_task_row
resource: crates/lpe-exchange/src/mapi/tables/collaboration_items.rs#L201-L220
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/task/task_property_value_with_reminder
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/search_folders/serialize_search_content_row
---

# Signature

`pub(in crate::mapi) fn serialize_reminder_task_row( task: &crate::mapi_store::MapiTask, reminder: Option<&lpe_storage::ClientReminder>, columns: &[u32], ) -> Vec<u8>`

# Calls

- [task_property_value_with_reminder](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/task/task_property_value_with_reminder.md)
- [write_mapi_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value.md)
- [write_property_default](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default.md)

# Called by

- [serialize_search_content_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/search_folders/serialize_search_content_row.md)
---
type: Rust Function
title: task_property_value
resource: crates/lpe-exchange/src/mapi/properties/task.rs#L3-L10
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/task/task_property_value_with_reminder
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_task
  - functions/crates/lpe-exchange/src/mapi/properties/tests/collaboration_item_properties_project_outlook_table_identity_columns
  - functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_task_row
---

# Signature

`pub(in crate::mapi) fn task_property_value( task: &ClientTask, item_id: u64, folder_id: u64, property_tag: u32, ) -> Option<MapiValue>`

# Calls

- [task_property_value_with_reminder](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/task/task_property_value_with_reminder.md)

# Called by

- [restriction_matches_task](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_task.md)
- [collaboration_item_properties_project_outlook_table_identity_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/collaboration_item_properties_project_outlook_table_identity_columns.md)
- [serialize_task_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_task_row.md)
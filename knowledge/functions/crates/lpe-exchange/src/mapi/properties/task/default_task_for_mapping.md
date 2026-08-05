---
type: Rust Function
title: default_task_for_mapping
resource: crates/lpe-exchange/src/mapi/properties/task.rs#L120-L146
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/default_mapping_rights
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response
  - functions/crates/lpe-exchange/src/mapi/properties/tests/collaboration_item_properties_project_outlook_table_identity_columns
  - functions/crates/lpe-exchange/src/mapi/properties/tests/microsoft_oxprops_message_size_projects_integer32_property
  - functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_task_row
---

# Signature

`pub(in crate::mapi) fn default_task_for_mapping( account_id: Uuid, collection_id: &str, ) -> ClientTask`

# Calls

- [default_mapping_rights](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/default_mapping_rights.md)

# Called by

- [append_save_changes_message_route_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response.md)
- [collaboration_item_properties_project_outlook_table_identity_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/collaboration_item_properties_project_outlook_table_identity_columns.md)
- [microsoft_oxprops_message_size_projects_integer32_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/microsoft_oxprops_message_size_projects_integer32_property.md)
- [serialize_pending_task_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_task_row.md)
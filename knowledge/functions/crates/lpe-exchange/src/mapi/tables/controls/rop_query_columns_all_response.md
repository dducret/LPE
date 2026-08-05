---
type: Rust Function
title: rop_query_columns_all_response
resource: crates/lpe-exchange/src/mapi/tables/controls.rs#L3-L65
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_folder_property_tags
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_navigation_shortcut_property_tags
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_message_property_tags
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_calendar_configuration_property_tags
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/should_use_associated_config_table
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_associated_config_columns
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_contact_property_tags
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_event_property_tags
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_task_property_tags
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_reminder_property_tags
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_note_property_tags
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_journal_entry_property_tags
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_attachment_columns
  - functions/crates/lpe-exchange/src/mapi/permissions/default_permission_columns
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_rule_columns
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/query_columns_all_response
---

# Signature

`pub(in crate::mapi) fn rop_query_columns_all_response( request: &RopRequest, object: Option<&MapiObject>, snapshot: &MapiMailStoreSnapshot, ) -> Vec<u8>`

# Calls

- [default_folder_property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_folder_property_tags.md)
- [default_navigation_shortcut_property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_navigation_shortcut_property_tags.md)
- [default_message_property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_message_property_tags.md)
- [collaboration_folder_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id.md)
- [default_calendar_configuration_property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_calendar_configuration_property_tags.md)
- [should_use_associated_config_table](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/should_use_associated_config_table.md)
- [default_associated_config_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_associated_config_columns.md)
- [default_contact_property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_contact_property_tags.md)
- [default_event_property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_event_property_tags.md)
- [default_task_property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_task_property_tags.md)
- [default_reminder_property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_reminder_property_tags.md)
- [default_note_property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_note_property_tags.md)
- [default_journal_entry_property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_journal_entry_property_tags.md)
- [default_attachment_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_attachment_columns.md)
- [default_permission_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/permissions/default_permission_columns.md)
- [default_rule_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_rule_columns.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)

# Called by

- [query_columns_all_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/query_columns_all_response.md)
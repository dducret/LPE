---
type: Rust Function
title: write_property_default
resource: crates/lpe-exchange/src/mapi/tables/row_codecs.rs#L213-L238
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_type
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u16
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u64
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_ascii_z
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_utf16z
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_rop_binary
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/normal_message_defaulted_column_detail
  - functions/crates/lpe-exchange/src/mapi/permissions/serialize_permission_row
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value
  - functions/crates/lpe-exchange/src/mapi/rop/fallback_default_specific_property
  - functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property
  - functions/crates/lpe-exchange/src/mapi/rop/debug/log_get_properties_specific_debug
  - functions/crates/lpe-exchange/src/mapi/rop/debug/format_property_value_shapes_for_debug
  - functions/crates/lpe-exchange/src/mapi/rop/event_properties/serialize_event_object_property
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_navigation_shortcut_row
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_search_folder_definition_row_with_mailbox_guid
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_common_view_named_view_row_with_mailbox_guid
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_conversation_action_row
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_freebusy_row_staged
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_associated_config_row_with_mailbox_guid
  - functions/crates/lpe-exchange/src/mapi/tables/attachments/serialize_attachment_row
  - functions/crates/lpe-exchange/src/mapi/tables/attachments/serialize_pending_attachment_row
  - functions/crates/lpe-exchange/src/mapi/tables/attachments/serialize_saved_attachment_row
  - functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_contact_row
  - functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_mapi_contact_row
  - functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_event_row_with_reminder_and_attachments
  - functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_versioned_event_row_with_reminder_and_attachments
  - functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_task_row
  - functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_reminder_task_row
  - functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_note_row
  - functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_journal_entry_row
  - functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_row_with_table_instance
  - functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_category_header_row
  - functions/crates/lpe-exchange/src/mapi/tables/deleted_items/serialize_categorized_deleted_event_row
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_advertised_special_folder_row_with_counts_and_change_number
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_root_folder_row
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_ipm_subtree_folder_row
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_logon_row
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_folder_row_with_context
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_collaboration_folder_row_with_context
  - functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_message_row
  - functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_associated_message_row
  - functions/crates/lpe-exchange/src/mapi/tables/public_folders/serialize_public_folder_row
  - functions/crates/lpe-exchange/src/mapi/tables/public_folders/serialize_public_folder_item_row
  - functions/crates/lpe-exchange/src/mapi/tables/recoverable_items/serialize_recoverable_item_row
  - functions/crates/lpe-exchange/src/mapi/tables/rules/serialize_rule_row
  - functions/crates/lpe-exchange/src/mapi/tables/tests/property_defaults_serialize_floating_types_with_wire_widths
  - functions/crates/lpe-exchange/src/mapi/tables/tests/property_defaults_serialize_server_ids_as_empty_counted_binary
  - functions/crates/lpe-exchange/src/mapi/tables/tests/property_defaults_serialize_multi_value_instance_columns
---

# Signature

`pub(in crate::mapi) fn write_property_default(row: &mut Vec<u8>, property_tag: u32)`

# Calls

- [property_type](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_type.md)
- [write_u16](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u16.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [write_u64](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u64.md)
- [write_ascii_z](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_ascii_z.md)
- [write_utf16z](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_utf16z.md)
- [write_rop_binary](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_rop_binary.md)

# Called by

- [normal_message_defaulted_column_detail](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/normal_message_defaulted_column_detail.md)
- [serialize_permission_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/permissions/serialize_permission_row.md)
- [write_mapi_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value.md)
- [fallback_default_specific_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/fallback_default_specific_property.md)
- [serialize_object_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property.md)
- [log_get_properties_specific_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/log_get_properties_specific_debug.md)
- [format_property_value_shapes_for_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_property_value_shapes_for_debug.md)
- [serialize_event_object_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/event_properties/serialize_event_object_property.md)
- [serialize_navigation_shortcut_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_navigation_shortcut_row.md)
- [serialize_search_folder_definition_row_with_mailbox_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_search_folder_definition_row_with_mailbox_guid.md)
- [serialize_common_view_named_view_row_with_mailbox_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_common_view_named_view_row_with_mailbox_guid.md)
- [serialize_conversation_action_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_conversation_action_row.md)
- [serialize_freebusy_row_staged](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_freebusy_row_staged.md)
- [serialize_associated_config_row_with_mailbox_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_associated_config_row_with_mailbox_guid.md)
- [serialize_attachment_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/attachments/serialize_attachment_row.md)
- [serialize_pending_attachment_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/attachments/serialize_pending_attachment_row.md)
- [serialize_saved_attachment_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/attachments/serialize_saved_attachment_row.md)
- [serialize_contact_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_contact_row.md)
- [serialize_mapi_contact_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_mapi_contact_row.md)
- [serialize_event_row_with_reminder_and_attachments](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_event_row_with_reminder_and_attachments.md)
- [serialize_versioned_event_row_with_reminder_and_attachments](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_versioned_event_row_with_reminder_and_attachments.md)
- [serialize_task_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_task_row.md)
- [serialize_reminder_task_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_reminder_task_row.md)
- [serialize_note_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_note_row.md)
- [serialize_journal_entry_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_journal_entry_row.md)
- [serialize_message_row_with_table_instance](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_row_with_table_instance.md)
- [serialize_category_header_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_category_header_row.md)
- [serialize_categorized_deleted_event_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/deleted_items/serialize_categorized_deleted_event_row.md)
- [serialize_advertised_special_folder_row_with_counts_and_change_number](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_advertised_special_folder_row_with_counts_and_change_number.md)
- [serialize_root_folder_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_root_folder_row.md)
- [serialize_ipm_subtree_folder_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_ipm_subtree_folder_row.md)
- [serialize_logon_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_logon_row.md)
- [serialize_folder_row_with_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_folder_row_with_context.md)
- [serialize_collaboration_folder_row_with_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_collaboration_folder_row_with_context.md)
- [serialize_pending_message_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_message_row.md)
- [serialize_pending_associated_message_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_associated_message_row.md)
- [serialize_public_folder_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/public_folders/serialize_public_folder_row.md)
- [serialize_public_folder_item_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/public_folders/serialize_public_folder_item_row.md)
- [serialize_recoverable_item_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/recoverable_items/serialize_recoverable_item_row.md)
- [serialize_rule_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rules/serialize_rule_row.md)
- [property_defaults_serialize_floating_types_with_wire_widths](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/property_defaults_serialize_floating_types_with_wire_widths.md)
- [property_defaults_serialize_server_ids_as_empty_counted_binary](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/property_defaults_serialize_server_ids_as_empty_counted_binary.md)
- [property_defaults_serialize_multi_value_instance_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/property_defaults_serialize_multi_value_instance_columns.md)
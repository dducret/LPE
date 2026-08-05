---
type: Rust Function
title: write_u64
resource: crates/lpe-exchange/src/mapi/rop/buffer.rs#L112-L114
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/permissions/serialize_permission_row
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_object_id
  - functions/crates/lpe-exchange/src/mapi/rop/logon/rop_logon_response_body
  - functions/crates/lpe-exchange/src/mapi/rop/logon/rop_public_folder_logon_response_body
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_seek_stream_response
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_copy_to_stream_response
  - functions/crates/lpe-exchange/src/mapi/tables/attachments/serialize_attachment_row
  - functions/crates/lpe-exchange/src/mapi/tables/attachments/serialize_pending_attachment_row
  - functions/crates/lpe-exchange/src/mapi/tables/attachments/serialize_saved_attachment_row
  - functions/crates/lpe-exchange/src/mapi/tables/collapse/rop_get_collapse_state_response
  - functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_row_with_table_instance
  - functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_category_header_row
  - functions/crates/lpe-exchange/src/mapi/tables/deleted_items/serialize_categorized_deleted_event_row
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_advertised_special_folder_row_with_counts_and_change_number
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_root_folder_row
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_ipm_subtree_folder_row
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_folder_row_with_context
  - functions/crates/lpe-exchange/src/mapi/tables/public_folders/serialize_public_folder_item_row
  - functions/crates/lpe-exchange/src/mapi/tables/recoverable_items/serialize_recoverable_item_row
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default
  - functions/crates/lpe-exchange/src/mapi/tables/rules/serialize_rule_row
  - functions/crates/lpe-exchange/src/mapi/tables/tests/captured_calendar_table_query_rows_projects_exact_requested_property_row
  - functions/crates/lpe-exchange/src/mapi/tables/tests/microsoft_table_bookmark_and_collapse_rops_require_set_columns
---

# Signature

`pub(in crate::mapi) fn write_u64(body: &mut Vec<u8>, value: u64)`

# Called by

- [serialize_permission_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/permissions/serialize_permission_row.md)
- [write_mapi_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value.md)
- [write_object_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_object_id.md)
- [rop_logon_response_body](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/logon/rop_logon_response_body.md)
- [rop_public_folder_logon_response_body](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/logon/rop_public_folder_logon_response_body.md)
- [rop_seek_stream_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_seek_stream_response.md)
- [rop_copy_to_stream_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_copy_to_stream_response.md)
- [serialize_attachment_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/attachments/serialize_attachment_row.md)
- [serialize_pending_attachment_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/attachments/serialize_pending_attachment_row.md)
- [serialize_saved_attachment_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/attachments/serialize_saved_attachment_row.md)
- [rop_get_collapse_state_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collapse/rop_get_collapse_state_response.md)
- [serialize_message_row_with_table_instance](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_row_with_table_instance.md)
- [serialize_category_header_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_category_header_row.md)
- [serialize_categorized_deleted_event_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/deleted_items/serialize_categorized_deleted_event_row.md)
- [serialize_advertised_special_folder_row_with_counts_and_change_number](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_advertised_special_folder_row_with_counts_and_change_number.md)
- [serialize_root_folder_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_root_folder_row.md)
- [serialize_ipm_subtree_folder_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_ipm_subtree_folder_row.md)
- [serialize_folder_row_with_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_folder_row_with_context.md)
- [serialize_public_folder_item_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/public_folders/serialize_public_folder_item_row.md)
- [serialize_recoverable_item_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/recoverable_items/serialize_recoverable_item_row.md)
- [write_property_default](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default.md)
- [serialize_rule_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rules/serialize_rule_row.md)
- [captured_calendar_table_query_rows_projects_exact_requested_property_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/captured_calendar_table_query_rows_projects_exact_requested_property_row.md)
- [microsoft_table_bookmark_and_collapse_rops_require_set_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/microsoft_table_bookmark_and_collapse_rops_require_set_columns.md)
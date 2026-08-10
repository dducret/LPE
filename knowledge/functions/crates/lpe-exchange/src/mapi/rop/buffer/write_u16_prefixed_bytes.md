---
type: Rust Function
title: write_u16_prefixed_bytes
resource: crates/lpe-exchange/src/mapi/rop/buffer.rs#L100-L103
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/request_reader/read_rop_request_with_logon_id
  - functions/crates/lpe-exchange/src/mapi/tables/attachments/serialize_attachment_row
  - functions/crates/lpe-exchange/src/mapi/tables/attachments/serialize_pending_attachment_row
  - functions/crates/lpe-exchange/src/mapi/tables/attachments/serialize_saved_attachment_row
  - functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_row_with_table_instance
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_advertised_special_folder_row_with_counts_and_change_number
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_root_folder_row
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_ipm_subtree_folder_row
  - functions/crates/lpe-exchange/src/mapi/tables/public_folders/serialize_public_folder_item_row
  - functions/crates/lpe-exchange/src/mapi/tables/recoverable_items/serialize_recoverable_item_row
  - functions/crates/lpe-exchange/src/mapi/tables/rules/serialize_rule_row
  - functions/crates/lpe-exchange/src/mapi/tables/tests/query_rows_truncates_variable_property_values_to_microsoft_limit
  - functions/crates/lpe-exchange/src/mapi/tables/tests/captured_calendar_table_query_rows_projects_exact_requested_property_row
---

# Signature

`pub(in crate::mapi) fn write_u16_prefixed_bytes(body: &mut Vec<u8>, value: &[u8])`

# Called by

- [read_rop_request_with_logon_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/request_reader/read_rop_request_with_logon_id.md)
- [serialize_attachment_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/attachments/serialize_attachment_row.md)
- [serialize_pending_attachment_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/attachments/serialize_pending_attachment_row.md)
- [serialize_saved_attachment_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/attachments/serialize_saved_attachment_row.md)
- [serialize_message_row_with_table_instance](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_row_with_table_instance.md)
- [serialize_advertised_special_folder_row_with_counts_and_change_number](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_advertised_special_folder_row_with_counts_and_change_number.md)
- [serialize_root_folder_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_root_folder_row.md)
- [serialize_ipm_subtree_folder_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_ipm_subtree_folder_row.md)
- [serialize_public_folder_item_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/public_folders/serialize_public_folder_item_row.md)
- [serialize_recoverable_item_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/recoverable_items/serialize_recoverable_item_row.md)
- [serialize_rule_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rules/serialize_rule_row.md)
- [query_rows_truncates_variable_property_values_to_microsoft_limit](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/query_rows_truncates_variable_property_values_to_microsoft_limit.md)
- [captured_calendar_table_query_rows_projects_exact_requested_property_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/captured_calendar_table_query_rows_projects_exact_requested_property_row.md)
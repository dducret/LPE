---
type: Rust Function
title: parse_mapi_property_value
resource: crates/lpe-exchange/src/mapi/properties/values.rs#L256-L378
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_type
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u16
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_i32
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_bytes
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u8
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_i64
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_ascii_z
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_utf16z
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_type_code
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/additional_ren_entry_ids_from_profile_bytes
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/hierarchy_response_metric_summary
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_hierarchy_query_rows_wire_summary
  - functions/crates/lpe-exchange/src/mapi/properties/tests/round_trip
  - functions/crates/lpe-exchange/src/mapi/properties/tests/unsupported_property_types_fail_explicitly
  - functions/crates/lpe-exchange/src/mapi/rop/debug/format_default_view_entry_id_decoding
  - functions/crates/lpe-exchange/src/mapi/rop/parse/parse_property_value_for_tag
  - functions/crates/lpe-exchange/src/mapi/rop/tests/saved_message_handle_getprops_keeps_batch_email_and_durable_identity
  - functions/crates/lpe-exchange/src/mapi/rop/tests/inbox_getprops_captured_unpersisted_folder_values_are_absent
  - functions/crates/lpe-exchange/src/mapi/tables/tests/query_rows_truncates_variable_property_values_to_microsoft_limit
  - functions/crates/lpe-exchange/src/mapi/tables/tests/assert_category_header_row
  - functions/crates/lpe-exchange/src/mapi/tables/tests/captured_common_views_query_rows_flags_heterogeneous_missing_columns
  - functions/crates/lpe-exchange/src/mapi/tables/tests/captured_calendar_fai_terminal_window_contains_only_canonical_configs
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_query_rows_uses_standard_property_rows_for_complete_rows
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_query_rows_default_columns_cover_required_configuration_contract
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_rows_project_folder_id_and_last_modification_time
  - functions/crates/lpe-exchange/src/mapi/tables/tests/persisted_inbox_named_view_associated_row_preserves_only_stored_view_properties
  - functions/crates/lpe-exchange/src/mapi/tables/tests/draft_message_row_projects_mf_unsent_from_canonical_mailbox_state
  - functions/crates/lpe-exchange/src/mapi/tables/tests/normal_inbox_query_rows_projects_sender_and_delivery_time
  - functions/crates/lpe-exchange/src/mapi/tables/tests/categorized_and_deleted_message_rows_keep_long_term_entry_ids
---

# Signature

`pub(in crate::mapi) fn parse_mapi_property_value( cursor: &mut Cursor<'_>, property_tag: u32, ) -> Result<MapiValue>`

# Calls

- [property_type](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_type.md)
- [read_u16](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u16.md)
- [read_i32](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_i32.md)
- [read_bytes](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_bytes.md)
- [read_u8](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u8.md)
- [read_i64](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_i64.md)
- [read_ascii_z](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_ascii_z.md)
- [read_utf16z](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_utf16z.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [property_type_code](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_type_code.md)

# Called by

- [additional_ren_entry_ids_from_profile_bytes](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/additional_ren_entry_ids_from_profile_bytes.md)
- [hierarchy_response_metric_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/hierarchy_response_metric_summary.md)
- [format_hierarchy_query_rows_wire_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_hierarchy_query_rows_wire_summary.md)
- [round_trip](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/round_trip.md)
- [unsupported_property_types_fail_explicitly](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/unsupported_property_types_fail_explicitly.md)
- [format_default_view_entry_id_decoding](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_default_view_entry_id_decoding.md)
- [parse_property_value_for_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/parse_property_value_for_tag.md)
- [saved_message_handle_getprops_keeps_batch_email_and_durable_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/saved_message_handle_getprops_keeps_batch_email_and_durable_identity.md)
- [inbox_getprops_captured_unpersisted_folder_values_are_absent](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/inbox_getprops_captured_unpersisted_folder_values_are_absent.md)
- [query_rows_truncates_variable_property_values_to_microsoft_limit](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/query_rows_truncates_variable_property_values_to_microsoft_limit.md)
- [assert_category_header_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/assert_category_header_row.md)
- [captured_common_views_query_rows_flags_heterogeneous_missing_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/captured_common_views_query_rows_flags_heterogeneous_missing_columns.md)
- [captured_calendar_fai_terminal_window_contains_only_canonical_configs](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/captured_calendar_fai_terminal_window_contains_only_canonical_configs.md)
- [inbox_associated_query_rows_uses_standard_property_rows_for_complete_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_query_rows_uses_standard_property_rows_for_complete_rows.md)
- [inbox_associated_query_rows_default_columns_cover_required_configuration_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_query_rows_default_columns_cover_required_configuration_contract.md)
- [inbox_associated_rows_project_folder_id_and_last_modification_time](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_rows_project_folder_id_and_last_modification_time.md)
- [persisted_inbox_named_view_associated_row_preserves_only_stored_view_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/persisted_inbox_named_view_associated_row_preserves_only_stored_view_properties.md)
- [draft_message_row_projects_mf_unsent_from_canonical_mailbox_state](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/draft_message_row_projects_mf_unsent_from_canonical_mailbox_state.md)
- [normal_inbox_query_rows_projects_sender_and_delivery_time](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/normal_inbox_query_rows_projects_sender_and_delivery_time.md)
- [categorized_and_deleted_message_rows_keep_long_term_entry_ids](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/categorized_and_deleted_message_rows_keep_long_term_entry_ids.md)
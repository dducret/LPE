---
type: Rust Method
title: read_u8
resource: crates/lpe-exchange/src/mapi/rop/buffer.rs#L35-L38
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_bytes
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_logon_response_rop
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/message/summarize_message_getprops_materialization
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/getprops_contract_response_summary
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/extract_getprops_binary_value
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/summarize_flagged_getprops_materialization
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/hierarchy_response_metric_summary
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_hierarchy_query_rows_wire_summary
  - functions/crates/lpe-exchange/src/mapi/nspi/parse_resolve_names_columns
  - functions/crates/lpe-exchange/src/mapi/nspi/parse_resolve_names_values
  - functions/crates/lpe-exchange/src/mapi/nspi/dn_to_mid/parse_dn_to_mid_names
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/parse_nspi_get_props_request
  - functions/crates/lpe-exchange/src/mapi/properties/values/parse_mapi_property_value
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_delete_source_keys
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_read_state_changes
  - functions/crates/lpe-exchange/src/mapi/rop/parse/parse_named_property
  - functions/crates/lpe-exchange/src/mapi/rop/property_rows/parse_modify_rows
  - functions/crates/lpe-exchange/src/mapi/rop/recipients/RopRequest/modify_recipients
  - functions/crates/lpe-exchange/src/mapi/rop/recipients/parse_wrapped_pending_recipient_row
  - functions/crates/lpe-exchange/src/mapi/rop/request_reader/read_rop_request_with_logon_id
  - functions/crates/lpe-exchange/src/mapi/rop/restrictions/parse_mapi_restriction_from
  - functions/crates/lpe-exchange/src/mapi/rop/tests/inbox_getprops_captured_unpersisted_folder_values_are_absent
---

# Signature

`pub(in crate::mapi) fn read_u8(&mut self) -> Result<u8>`

# Calls

- [read_bytes](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_bytes.md)

# Called by

- [summarize_logon_response_rop](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_logon_response_rop.md)
- [summarize_message_getprops_materialization](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/message/summarize_message_getprops_materialization.md)
- [getprops_contract_response_summary](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/getprops_contract_response_summary.md)
- [extract_getprops_binary_value](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/extract_getprops_binary_value.md)
- [summarize_flagged_getprops_materialization](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/summarize_flagged_getprops_materialization.md)
- [hierarchy_response_metric_summary](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/hierarchy_response_metric_summary.md)
- [format_hierarchy_query_rows_wire_summary](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_hierarchy_query_rows_wire_summary.md)
- [parse_resolve_names_columns](../../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/parse_resolve_names_columns.md)
- [parse_resolve_names_values](../../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/parse_resolve_names_values.md)
- [parse_dn_to_mid_names](../../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/dn_to_mid/parse_dn_to_mid_names.md)
- [parse_nspi_get_props_request](../../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/parse_nspi_get_props_request.md)
- [parse_mapi_property_value](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/parse_mapi_property_value.md)
- [import_delete_source_keys](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_delete_source_keys.md)
- [import_read_state_changes](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_read_state_changes.md)
- [parse_named_property](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/parse_named_property.md)
- [parse_modify_rows](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/property_rows/parse_modify_rows.md)
- [modify_recipients](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/recipients/RopRequest/modify_recipients.md)
- [parse_wrapped_pending_recipient_row](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/recipients/parse_wrapped_pending_recipient_row.md)
- [read_rop_request_with_logon_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/request_reader/read_rop_request_with_logon_id.md)
- [parse_mapi_restriction_from](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/restrictions/parse_mapi_restriction_from.md)
- [inbox_getprops_captured_unpersisted_folder_values_are_absent](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/inbox_getprops_captured_unpersisted_folder_values_are_absent.md)
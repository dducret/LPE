---
type: Rust Method
title: input_handle_index
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L35-L37
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/probes/summarize_first_post_hierarchy_probe
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/probes/set_properties_probe_request
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/log_get_properties_default_folder_response_debug
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/log_get_properties_specific_response_debug
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/rop_buffer_is_store_independent_special_folder_getprops_probe
  - functions/crates/lpe-exchange/src/mapi/dispatch/logon/private_logon_request_handle
  - functions/crates/lpe-exchange/src/mapi/dispatch/release/append_release_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_table_control_dispatch_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_set_columns_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_sort_table_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_restrict_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_query_rows_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_find_row_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_open/append_open_table_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/apply_outlook_smart_input_variant_before_query_rows
  - functions/crates/lpe-exchange/src/mapi/properties/rop_read_recipients_response
  - functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom
  - functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_all_response
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_read_stream_response
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_seek_stream_response
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_save_changes_message_response
  - functions/crates/lpe-exchange/src/mapi/session/input_handle
  - functions/crates/lpe-exchange/src/mapi/session/release_handle_slot
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/hierarchy_sync_selective_fallback_plan
---

# Signature

`pub(in crate::mapi) fn input_handle_index(&self) -> Option<u8>`

# Called by

- [execute_rops](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops.md)
- [summarize_first_post_hierarchy_probe](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/probes/summarize_first_post_hierarchy_probe.md)
- [set_properties_probe_request](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/probes/set_properties_probe_request.md)
- [log_get_properties_default_folder_response_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/log_get_properties_default_folder_response_debug.md)
- [log_get_properties_specific_response_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/property_responses/log_get_properties_specific_response_debug.md)
- [rop_buffer_is_store_independent_special_folder_getprops_probe](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/rop_buffer_is_store_independent_special_folder_getprops_probe.md)
- [private_logon_request_handle](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/logon/private_logon_request_handle.md)
- [append_release_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/release/append_release_response.md)
- [append_table_control_dispatch_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_table_control_dispatch_response.md)
- [append_set_columns_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_set_columns_response.md)
- [append_sort_table_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_sort_table_response.md)
- [append_restrict_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_restrict_response.md)
- [append_query_rows_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_query_rows_response.md)
- [append_find_row_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_find_row_response.md)
- [append_open_table_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_open/append_open_table_response.md)
- [apply_outlook_smart_input_variant_before_query_rows](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/apply_outlook_smart_input_variant_before_query_rows.md)
- [rop_read_recipients_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/rop_read_recipients_response.md)
- [rop_get_properties_specific_response_with_custom](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom.md)
- [rop_get_properties_all_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_all_response.md)
- [rop_read_stream_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_read_stream_response.md)
- [rop_seek_stream_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_seek_stream_response.md)
- [rop_save_changes_message_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_save_changes_message_response.md)
- [input_handle](../../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_handle.md)
- [release_handle_slot](../../../../../../../../functions/crates/lpe-exchange/src/mapi/session/release_handle_slot.md)
- [hierarchy_sync_selective_fallback_plan](../../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/hierarchy_sync_selective_fallback_plan.md)
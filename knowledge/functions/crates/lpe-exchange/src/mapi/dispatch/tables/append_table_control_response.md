---
type: Rust Function
title: append_table_control_response
resource: crates/lpe-exchange/src/mapi/dispatch/tables.rs#L1408-L1908
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/get_status_response
  - functions/crates/lpe-exchange/src/mapi/session/input_object
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/outlook_view_descriptor_visible_property_tags
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_calendar_event_query_position_summary
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/open_folder/debug_open_folder_metadata
  - functions/crates/lpe-exchange/src/mapi/properties/views/default_view_supported_folder
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/debug_role_for_folder_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/query_position_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_mapi_query_position_debug
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar_contract/log_calendar_view_contract_fingerprint
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_calendar_normal_contents_table_query_position
  - functions/crates/lpe-exchange/src/mapi/session/input_handle
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_view_failure_trace_event
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_calendar_query_position_wire_summary
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_default_view_query_position_wire_summary
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_hierarchy_table_query_position_context
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_normal_contents_table_query_position
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_visible_inbox_query_position_wire_summary
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/named_properties/format_contents_table_named_property_context
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/seek_row_response
  - functions/crates/lpe-exchange/src/mapi/session/input_object_mut
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_seek_row
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/seek_row_bookmark_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/seek_row_fractional_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/create_bookmark_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/query_columns_all_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/expand_row_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/collapse_row_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/get_collapse_state_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/set_collapse_state_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_table_control_dispatch_response
---

# Signature

`pub(super) fn append_table_control_response( principal: &AccountPrincipal, request_id: &str, request_rop_names: &str, session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, )`

# Calls

- [get_status_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/get_status_response.md)
- [input_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)
- [outlook_view_descriptor_visible_property_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/outlook_view_descriptor_visible_property_tags.md)
- [format_calendar_event_query_position_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_calendar_event_query_position_summary.md)
- [debug_open_folder_metadata](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/open_folder/debug_open_folder_metadata.md)
- [default_view_supported_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/default_view_supported_folder.md)
- [debug_role_for_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/debug_role_for_folder_id.md)
- [query_position_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/query_position_response.md)
- [log_mapi_query_position_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_mapi_query_position_debug.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [log_calendar_view_contract_fingerprint](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar_contract/log_calendar_view_contract_fingerprint.md)
- [record_calendar_normal_contents_table_query_position](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_calendar_normal_contents_table_query_position.md)
- [input_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_handle.md)
- [record_outlook_view_failure_trace_event](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_view_failure_trace_event.md)
- [format_calendar_query_position_wire_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_calendar_query_position_wire_summary.md)
- [format_default_view_query_position_wire_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_default_view_query_position_wire_summary.md)
- [record_last_hierarchy_table_query_position_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_hierarchy_table_query_position_context.md)
- [record_inbox_normal_contents_table_query_position](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_normal_contents_table_query_position.md)
- [format_visible_inbox_query_position_wire_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_visible_inbox_query_position_wire_summary.md)
- [format_contents_table_named_property_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/named_properties/format_contents_table_named_property_context.md)
- [seek_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/seek_row_response.md)
- [input_object_mut](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object_mut.md)
- [log_outlook_contents_table_seek_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_seek_row.md)
- [seek_row_bookmark_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/seek_row_bookmark_response.md)
- [seek_row_fractional_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/seek_row_fractional_response.md)
- [create_bookmark_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/create_bookmark_response.md)
- [query_columns_all_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/query_columns_all_response.md)
- [expand_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/expand_row_response.md)
- [collapse_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/collapse_row_response.md)
- [get_collapse_state_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/get_collapse_state_response.md)
- [set_collapse_state_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/set_collapse_state_response.md)

# Called by

- [append_table_control_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_table_control_dispatch_response.md)
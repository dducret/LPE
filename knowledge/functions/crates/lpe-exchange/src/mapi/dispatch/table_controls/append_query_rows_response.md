---
type: Rust Function
title: append_query_rows_response
resource: crates/lpe-exchange/src/mapi/dispatch/table_controls.rs#L882-L1386
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_handle
  - functions/crates/lpe-exchange/src/mapi/session/input_object
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/open_folder/debug_open_folder_metadata
  - functions/crates/lpe-exchange/src/mapi/properties/views/default_view_supported_folder
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/outlook_bootstrap_query_rows_phase
  - functions/crates/lpe-exchange/src/mapi/tables/diagnostics/outlook_bootstrap_row_invariant_summaries
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/query_forward_read
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/query_row_count
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/outlook_bootstrap_query_rows_total_count
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/named_properties/format_contents_table_named_property_context
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar/log_calendar_hierarchy_query_rows_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_query_rows
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_inbox_associated_query_context
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_common_views_inbox_shortcut_context
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_inbox_hierarchy_query_context
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_inbox_associated_query_context
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_common_views_inbox_shortcut_context
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_inbox_hierarchy_query_context
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/apply_outlook_smart_input_variant_before_query_rows
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/query_rows_response
  - functions/crates/lpe-exchange/src/mapi/session/input_object_mut
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_query_rows_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_hierarchy_table_query_rows_response
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_hierarchy_columns
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/hierarchy_response/format_hierarchy_query_rows_wire_summary
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_view_failure_trace_event
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_table_query_rows_context
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_associated_non_empty_query_context
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_associated_query_rows_reached_end
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/format_inbox_post_fai_handoff_context
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/format_live_handle_debug_summary
  - functions/crates/lpe-exchange/src/mapi/record_mapi_outlook_view_inbox_fai_handoff_without_contents
  - functions/crates/lpe-exchange/src/mapi/record_mapi_outlook_view_bootstrap_stall
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/mark_post_inbox_fai_handoff_logged
  - functions/crates/lpe-exchange/src/mapi/record_mapi_outlook_view_common_views_handoff_without_contents
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/mark_post_common_views_handoff_logged
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_associated_query_rows_returned_non_empty
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/log_outlook_bootstrap_phase
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/log_outlook_bootstrap_row_invariant
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_normal_contents_table_query_rows
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_lifecycle/record_normal_inbox_table_lifecycle
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/input_handle_index
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_calendar_normal_contents_table_query_rows
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_default_view_normal_contents_table_query_rows
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_table_control_dispatch_response
---

# Signature

`pub(super) fn append_query_rows_response( principal: &AccountPrincipal, session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, request_id: &str, request_rop_names: &str, mailboxes: &[lpe_storage::JmapMailbox], emails: &[lpe_storage::JmapEmail], snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, )`

# Calls

- [input_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_handle.md)
- [input_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)
- [debug_open_folder_metadata](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/open_folder/debug_open_folder_metadata.md)
- [default_view_supported_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/default_view_supported_folder.md)
- [outlook_bootstrap_query_rows_phase](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/outlook_bootstrap_query_rows_phase.md)
- [outlook_bootstrap_row_invariant_summaries](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/diagnostics/outlook_bootstrap_row_invariant_summaries.md)
- [query_forward_read](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/query_forward_read.md)
- [query_row_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/query_row_count.md)
- [outlook_bootstrap_query_rows_total_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/outlook_bootstrap_query_rows_total_count.md)
- [format_contents_table_named_property_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/named_properties/format_contents_table_named_property_context.md)
- [log_calendar_hierarchy_query_rows_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar/log_calendar_hierarchy_query_rows_contract.md)
- [log_outlook_contents_table_query_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_query_rows.md)
- [format_inbox_associated_query_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_inbox_associated_query_context.md)
- [format_common_views_inbox_shortcut_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_common_views_inbox_shortcut_context.md)
- [format_inbox_hierarchy_query_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_inbox_hierarchy_query_context.md)
- [record_last_inbox_associated_query_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_inbox_associated_query_context.md)
- [record_last_common_views_inbox_shortcut_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_common_views_inbox_shortcut_context.md)
- [record_last_inbox_hierarchy_query_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_inbox_hierarchy_query_context.md)
- [apply_outlook_smart_input_variant_before_query_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/apply_outlook_smart_input_variant_before_query_rows.md)
- [query_rows_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/query_rows_response.md)
- [input_object_mut](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object_mut.md)
- [log_outlook_contents_table_query_rows_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_query_rows_response.md)
- [log_outlook_hierarchy_table_query_rows_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_hierarchy_table_query_rows_response.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [default_hierarchy_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_hierarchy_columns.md)
- [format_hierarchy_query_rows_wire_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/hierarchy_response/format_hierarchy_query_rows_wire_summary.md)
- [record_outlook_view_failure_trace_event](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_view_failure_trace_event.md)
- [record_last_table_query_rows_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_table_query_rows_context.md)
- [record_inbox_associated_non_empty_query_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_associated_non_empty_query_context.md)
- [record_inbox_associated_query_rows_reached_end](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_associated_query_rows_reached_end.md)
- [format_inbox_post_fai_handoff_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/format_inbox_post_fai_handoff_context.md)
- [format_live_handle_debug_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/format_live_handle_debug_summary.md)
- [record_mapi_outlook_view_inbox_fai_handoff_without_contents](../../../../../../../functions/crates/lpe-exchange/src/mapi/record_mapi_outlook_view_inbox_fai_handoff_without_contents.md)
- [record_mapi_outlook_view_bootstrap_stall](../../../../../../../functions/crates/lpe-exchange/src/mapi/record_mapi_outlook_view_bootstrap_stall.md)
- [mark_post_inbox_fai_handoff_logged](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/mark_post_inbox_fai_handoff_logged.md)
- [record_mapi_outlook_view_common_views_handoff_without_contents](../../../../../../../functions/crates/lpe-exchange/src/mapi/record_mapi_outlook_view_common_views_handoff_without_contents.md)
- [mark_post_common_views_handoff_logged](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/mark_post_common_views_handoff_logged.md)
- [record_inbox_associated_query_rows_returned_non_empty](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_associated_query_rows_returned_non_empty.md)
- [log_outlook_bootstrap_phase](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/log_outlook_bootstrap_phase.md)
- [log_outlook_bootstrap_row_invariant](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/log_outlook_bootstrap_row_invariant.md)
- [record_inbox_normal_contents_table_query_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_inbox_normal_contents_table_query_rows.md)
- [record_normal_inbox_table_lifecycle](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_lifecycle/record_normal_inbox_table_lifecycle.md)
- [input_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/input_handle_index.md)
- [record_calendar_normal_contents_table_query_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_calendar_normal_contents_table_query_rows.md)
- [record_default_view_normal_contents_table_query_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_default_view_normal_contents_table_query_rows.md)

# Called by

- [append_table_control_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_table_control_dispatch_response.md)
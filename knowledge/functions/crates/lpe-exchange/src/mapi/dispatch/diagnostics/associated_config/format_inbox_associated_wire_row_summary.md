---
type: Rust Function
title: format_inbox_associated_wire_row_summary
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/associated_config.rs#L70-L129
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_associated_table_rows
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/sort_debug_associated_table_rows
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/select_query_window
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/serialize_debug_associated_row
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/standard_property_row_bytes
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/query_rows_property_row_bytes
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_find_row
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_query_rows
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_query_rows_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/associated_config_wire_summary_uses_requested_position
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/associated_config_debug_summaries_honor_table_restriction
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/inbox_associated_named_view_debug_summaries_do_not_invent_folder_local_default_view
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/quick_step_associated_debug_summaries_do_not_report_synthetic_custom_action_row
---

# Signature

`pub(in crate::mapi::dispatch) fn format_inbox_associated_wire_row_summary( mailbox_guid: Uuid, folder_id: u64, associated: bool, position: usize, forward_read: bool, row_count: usize, sort_orders: &[MapiSortOrder], restriction: Option<&MapiRestriction>, columns: &[u32], snapshot: &MapiMailStoreSnapshot, ) -> String`

# Calls

- [debug_associated_table_rows](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_associated_table_rows.md)
- [sort_debug_associated_table_rows](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/sort_debug_associated_table_rows.md)
- [select_query_window](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/select_query_window.md)
- [serialize_debug_associated_row](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/serialize_debug_associated_row.md)
- [standard_property_row_bytes](../../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/standard_property_row_bytes.md)
- [query_rows_property_row_bytes](../../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/query_rows_property_row_bytes.md)

# Called by

- [log_outlook_contents_table_find_row](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_find_row.md)
- [log_outlook_contents_table_query_rows](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_query_rows.md)
- [log_outlook_contents_table_query_rows_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_query_rows_response.md)
- [associated_config_wire_summary_uses_requested_position](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/associated_config_wire_summary_uses_requested_position.md)
- [associated_config_debug_summaries_honor_table_restriction](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/associated_config_debug_summaries_honor_table_restriction.md)
- [inbox_associated_named_view_debug_summaries_do_not_invent_folder_local_default_view](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/inbox_associated_named_view_debug_summaries_do_not_invent_folder_local_default_view.md)
- [quick_step_associated_debug_summaries_do_not_report_synthetic_custom_action_row](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/quick_step_associated_debug_summaries_do_not_report_synthetic_custom_action_row.md)
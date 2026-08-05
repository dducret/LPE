---
type: Rust Function
title: format_calendar_event_query_position_summary
resource: crates/lpe-exchange/src/mapi/dispatch/tables.rs#L796-L854
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/calendar/calendar_content_rows
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_events
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/select_query_window
  - functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_event_row
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/standard_property_row_bytes
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/event_property_value
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_debug_mapi_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar_contract/format_calendar_view_contract_fingerprint
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_mapi_query_position_debug
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/append_table_control_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/calendar_query_position_summary_projects_observed_outlook_columns
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/calendar_query_position_summary_flags_zero_duration_timed_events
---

# Signature

`pub(super) fn format_calendar_event_query_position_summary( folder_id: u64, associated: bool, position: usize, row_count: usize, sort_orders: &[MapiSortOrder], restriction: Option<&MapiRestriction>, columns: &[u32], snapshot: &MapiMailStoreSnapshot, ) -> String`

# Calls

- [calendar_content_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/calendar/calendar_content_rows.md)
- [sort_events](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_events.md)
- [select_query_window](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/select_query_window.md)
- [serialize_event_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collaboration_items/serialize_event_row.md)
- [standard_property_row_bytes](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/standard_property_row_bytes.md)
- [event_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/event_property_value.md)
- [format_debug_mapi_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_debug_mapi_value.md)

# Called by

- [format_calendar_view_contract_fingerprint](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar_contract/format_calendar_view_contract_fingerprint.md)
- [log_mapi_query_position_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_mapi_query_position_debug.md)
- [append_table_control_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/append_table_control_response.md)
- [calendar_query_position_summary_projects_observed_outlook_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/calendar_query_position_summary_projects_observed_outlook_columns.md)
- [calendar_query_position_summary_flags_zero_duration_timed_events](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/calendar_query_position_summary_flags_zero_duration_timed_events.md)
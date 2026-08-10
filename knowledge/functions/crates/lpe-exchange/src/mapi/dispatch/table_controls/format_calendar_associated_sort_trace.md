---
type: Rust Function
title: format_calendar_associated_sort_trace
resource: crates/lpe-exchange/src/mapi/dispatch/table_controls.rs#L702-L715
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_sort_table_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/calendar_associated_sort_trace_reports_missing_query_rows_handoff
---

# Signature

`pub(super) fn format_calendar_associated_sort_trace( request_id: &str, handle: String, columns: &[u32], sort_orders: &[MapiSortOrder], snapshot: &MapiMailStoreSnapshot, ) -> String`

# Called by

- [append_sort_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_sort_table_response.md)
- [calendar_associated_sort_trace_reports_missing_query_rows_handoff](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/calendar_associated_sort_trace_reports_missing_query_rows_handoff.md)
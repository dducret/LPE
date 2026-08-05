---
type: Rust Function
title: sort_events
resource: crates/lpe-exchange/src/mapi/tables/sorting.rs#L258-L297
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/compare_case_insensitive
  - functions/crates/lpe-exchange/src/mapi/tables/time/event_end_filetime
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/apply_sort_direction
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_calendar_event_query_position_summary
  - functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
  - functions/crates/lpe-exchange/src/mapi/tables/row_keys/table_row_keys
  - functions/crates/lpe-exchange/src/mapi/tables/tests/calendar_sort_uses_only_backed_calendar_values
---

# Signature

`pub(in crate::mapi) fn sort_events( rows: &mut [&crate::mapi_store::MapiEvent], sort_orders: &[MapiSortOrder], )`

# Calls

- [compare_case_insensitive](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/compare_case_insensitive.md)
- [event_end_filetime](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/event_end_filetime.md)
- [apply_sort_direction](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/apply_sort_direction.md)

# Called by

- [format_calendar_event_query_position_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_calendar_event_query_position_summary.md)
- [rop_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)
- [rop_query_rows_response_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)
- [table_row_keys](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_keys/table_row_keys.md)
- [calendar_sort_uses_only_backed_calendar_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/calendar_sort_uses_only_backed_calendar_values.md)
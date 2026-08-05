---
type: Rust Function
title: calendar_content_rows
resource: crates/lpe-exchange/src/mapi/tables/calendar.rs#L3-L11
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/events_for_folder
  - functions/crates/lpe-exchange/src/mapi/tables/calendar/restriction_matches_event
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_calendar_event_query_position_summary
  - functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/counts/table_position_and_count
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
  - functions/crates/lpe-exchange/src/mapi/tables/row_keys/table_row_keys
---

# Signature

`pub(in crate::mapi) fn calendar_content_rows<'a>( snapshot: &'a MapiMailStoreSnapshot, folder_id: u64, restriction: Option<&MapiRestriction>, ) -> Vec<&'a crate::mapi_store::MapiEvent>`

# Calls

- [events_for_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/events_for_folder.md)
- [restriction_matches_event](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/calendar/restriction_matches_event.md)

# Called by

- [format_calendar_event_query_position_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_calendar_event_query_position_summary.md)
- [rop_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)
- [table_position_and_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/table_position_and_count.md)
- [rop_query_rows_response_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)
- [table_row_keys](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_keys/table_row_keys.md)
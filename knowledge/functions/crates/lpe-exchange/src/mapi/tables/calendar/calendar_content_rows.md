---
type: Rust Function
title: calendar_content_rows
resource: crates/lpe-exchange/src/mapi/tables/calendar.rs#L3-L9
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/calendar/calendar_content_rows_with_mailbox_guid
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_calendar_event_query_position_summary
---

# Signature

`pub(in crate::mapi) fn calendar_content_rows<'a>( snapshot: &'a MapiMailStoreSnapshot, folder_id: u64, restriction: Option<&MapiRestriction>, ) -> Vec<&'a crate::mapi_store::MapiEvent>`

# Calls

- [calendar_content_rows_with_mailbox_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/calendar/calendar_content_rows_with_mailbox_guid.md)

# Called by

- [format_calendar_event_query_position_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_calendar_event_query_position_summary.md)
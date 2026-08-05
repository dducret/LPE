---
type: Rust Function
title: event_matches_filter
resource: crates/lpe-jmap/src/calendar.rs#L1055-L1095
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/calendar/calendar_event_start
  - functions/crates/lpe-jmap/src/parse/parse_local_datetime_value
  called_by:
  - functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_event_query
  - functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_event_query_changes
---

# Signature

`fn event_matches_filter(event: &AccessibleEvent, filter: &CalendarEventQueryFilter) -> bool`

# Calls

- [calendar_event_start](../../../../../functions/crates/lpe-jmap/src/calendar/calendar_event_start.md)
- [parse_local_datetime_value](../../../../../functions/crates/lpe-jmap/src/parse/parse_local_datetime_value.md)

# Called by

- [handle_calendar_event_query](../../../../../functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_event_query.md)
- [handle_calendar_event_query_changes](../../../../../functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_event_query_changes.md)
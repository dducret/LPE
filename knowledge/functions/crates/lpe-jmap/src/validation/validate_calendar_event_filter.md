---
type: Rust Function
title: validate_calendar_event_filter
resource: crates/lpe-jmap/src/validation.rs#L46-L61
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/parse/parse_local_datetime
  called_by:
  - functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_event_query
  - functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_event_query_changes
---

# Signature

`pub(crate) fn validate_calendar_event_filter( filter: Option<&CalendarEventQueryFilter>, ) -> Result<()>`

# Calls

- [parse_local_datetime](../../../../../functions/crates/lpe-jmap/src/parse/parse_local_datetime.md)

# Called by

- [handle_calendar_event_query](../../../../../functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_event_query.md)
- [handle_calendar_event_query_changes](../../../../../functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_event_query_changes.md)
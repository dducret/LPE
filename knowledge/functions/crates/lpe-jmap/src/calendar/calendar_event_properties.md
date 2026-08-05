---
type: Rust Function
title: calendar_event_properties
resource: crates/lpe-jmap/src/calendar.rs#L783-L812
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_event_get
  - functions/crates/lpe-jmap/src/calendar/JmapService/calendar_event_update_input
---

# Signature

`fn calendar_event_properties(properties: Option<Vec<String>>) -> HashSet<String>`

# Called by

- [handle_calendar_event_get](../../../../../functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_event_get.md)
- [calendar_event_update_input](../../../../../functions/crates/lpe-jmap/src/calendar/JmapService/calendar_event_update_input.md)
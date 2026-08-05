---
type: Rust Function
title: calendar_properties
resource: crates/lpe-jmap/src/calendar.rs#L738-L752
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_get
  - functions/crates/lpe-jmap/src/calendar/JmapService/calendar_update_name
---

# Signature

`fn calendar_properties(properties: Option<Vec<String>>) -> HashSet<String>`

# Called by

- [handle_calendar_get](../../../../../functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_get.md)
- [calendar_update_name](../../../../../functions/crates/lpe-jmap/src/calendar/JmapService/calendar_update_name.md)
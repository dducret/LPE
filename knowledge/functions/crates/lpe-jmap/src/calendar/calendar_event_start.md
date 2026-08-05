---
type: Rust Function
title: calendar_event_start
resource: crates/lpe-jmap/src/calendar.rs#L1101-L1103
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/calendar/event_matches_filter
  - functions/crates/lpe-jmap/src/calendar/calendar_event_sort_key
---

# Signature

`fn calendar_event_start(event: &AccessibleEvent) -> String`

# Called by

- [event_matches_filter](../../../../../functions/crates/lpe-jmap/src/calendar/event_matches_filter.md)
- [calendar_event_sort_key](../../../../../functions/crates/lpe-jmap/src/calendar/calendar_event_sort_key.md)
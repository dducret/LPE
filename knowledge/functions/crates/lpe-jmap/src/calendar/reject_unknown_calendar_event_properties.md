---
type: Rust Function
title: reject_unknown_calendar_event_properties
resource: crates/lpe-jmap/src/calendar.rs#L1252-L1280
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/calendar/parse_calendar_event_input
---

# Signature

`fn reject_unknown_calendar_event_properties(object: &Map<String, Value>) -> Result<()>`

# Called by

- [parse_calendar_event_input](../../../../../functions/crates/lpe-jmap/src/calendar/parse_calendar_event_input.md)
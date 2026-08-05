---
type: Rust Function
title: parse_calendar_location
resource: crates/lpe-jmap/src/calendar.rs#L1299-L1301
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/parse/parse_first_property_object_string
  called_by:
  - functions/crates/lpe-jmap/src/calendar/parse_calendar_event_input
---

# Signature

`fn parse_calendar_location(value: Option<&Value>) -> Result<String>`

# Calls

- [parse_first_property_object_string](../../../../../functions/crates/lpe-jmap/src/parse/parse_first_property_object_string.md)

# Called by

- [parse_calendar_event_input](../../../../../functions/crates/lpe-jmap/src/calendar/parse_calendar_event_input.md)
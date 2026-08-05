---
type: Rust Function
title: validate_calendar_ids
resource: crates/lpe-jmap/src/calendar.rs#L1282-L1297
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/as_bool
  called_by:
  - functions/crates/lpe-jmap/src/calendar/parse_calendar_event_input
---

# Signature

`fn validate_calendar_ids(value: Option<&Value>) -> Result<Option<String>>`

# Calls

- [next](../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [as_bool](../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/as_bool.md)

# Called by

- [parse_calendar_event_input](../../../../../functions/crates/lpe-jmap/src/calendar/parse_calendar_event_input.md)
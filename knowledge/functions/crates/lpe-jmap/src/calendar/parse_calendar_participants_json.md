---
type: Rust Function
title: parse_calendar_participants_json
resource: crates/lpe-jmap/src/calendar.rs#L1309-L1313
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/calendar/serialize_calendar_participants_metadata
  - functions/crates/lpe-jmap/src/calendar/parse_jmap_calendar_participants
  called_by:
  - functions/crates/lpe-jmap/src/calendar/parse_calendar_event_input
---

# Signature

`fn parse_calendar_participants_json(value: Option<&Value>) -> Result<String>`

# Calls

- [serialize_calendar_participants_metadata](../../../../../functions/crates/lpe-storage/src/calendar/serialize_calendar_participants_metadata.md)
- [parse_jmap_calendar_participants](../../../../../functions/crates/lpe-jmap/src/calendar/parse_jmap_calendar_participants.md)

# Called by

- [parse_calendar_event_input](../../../../../functions/crates/lpe-jmap/src/calendar/parse_calendar_event_input.md)
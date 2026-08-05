---
type: Rust Function
title: parse_calendar_participants
resource: crates/lpe-jmap/src/calendar.rs#L1303-L1307
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/calendar/calendar_attendee_labels
  - functions/crates/lpe-jmap/src/calendar/parse_jmap_calendar_participants
  called_by:
  - functions/crates/lpe-jmap/src/calendar/parse_calendar_event_input
---

# Signature

`fn parse_calendar_participants(value: Option<&Value>) -> Result<String>`

# Calls

- [calendar_attendee_labels](../../../../../functions/crates/lpe-storage/src/calendar/calendar_attendee_labels.md)
- [parse_jmap_calendar_participants](../../../../../functions/crates/lpe-jmap/src/calendar/parse_jmap_calendar_participants.md)

# Called by

- [parse_calendar_event_input](../../../../../functions/crates/lpe-jmap/src/calendar/parse_calendar_event_input.md)
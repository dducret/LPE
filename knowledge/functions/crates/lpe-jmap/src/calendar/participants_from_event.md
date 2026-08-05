---
type: Rust Function
title: participants_from_event
resource: crates/lpe-jmap/src/calendar.rs#L954-L990
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/calendar/parse_calendar_participants_metadata
  - functions/crates/lpe-jmap/src/calendar/participant_value
  - functions/crates/lpe-jmap/src/calendar/participants_from_attendees
  called_by:
  - functions/crates/lpe-jmap/src/calendar/calendar_event_to_value
---

# Signature

`fn participants_from_event(event: &AccessibleEvent) -> Value`

# Calls

- [parse_calendar_participants_metadata](../../../../../functions/crates/lpe-storage/src/calendar/parse_calendar_participants_metadata.md)
- [participant_value](../../../../../functions/crates/lpe-jmap/src/calendar/participant_value.md)
- [participants_from_attendees](../../../../../functions/crates/lpe-jmap/src/calendar/participants_from_attendees.md)

# Called by

- [calendar_event_to_value](../../../../../functions/crates/lpe-jmap/src/calendar/calendar_event_to_value.md)
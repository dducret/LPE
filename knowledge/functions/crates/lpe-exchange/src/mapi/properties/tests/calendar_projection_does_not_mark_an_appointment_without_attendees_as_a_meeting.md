---
type: Rust Function
title: calendar_projection_does_not_mark_an_appointment_without_attendees_as_a_meeting
resource: crates/lpe-exchange/src/mapi/properties/tests.rs#L3773-L3806
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/default_event_for_mapping
  - functions/crates/lpe-storage/src/calendar/serialize_calendar_participants_metadata
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/event_input_from_mapi
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
---

# Signature

`fn calendar_projection_does_not_mark_an_appointment_without_attendees_as_a_meeting()`

# Calls

- [default_event_for_mapping](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/default_event_for_mapping.md)
- [serialize_calendar_participants_metadata](../../../../../../../functions/crates/lpe-storage/src/calendar/serialize_calendar_participants_metadata.md)
- [event_input_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/event_input_from_mapi.md)
- [from_str](../../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)
---
type: Rust Function
title: event_participants_from_mapi
resource: crates/lpe-exchange/src/mapi/properties/calendar.rs#L798-L821
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/calendar/parse_calendar_participants_metadata
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/organizer_from_mapi
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/attendees_from_mapi
  - functions/crates/lpe-storage/src/calendar/serialize_calendar_participants_metadata
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/meeting/organizer_json_from_mapi
  - functions/crates/lpe-storage/src/calendar/calendar_attendee_labels
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/event_input_from_mapi
---

# Signature

`fn event_participants_from_mapi( existing: &AccessibleEvent, properties: &HashMap<u32, MapiValue>, ) -> MapiEventParticipants`

# Calls

- [parse_calendar_participants_metadata](../../../../../../../functions/crates/lpe-storage/src/calendar/parse_calendar_participants_metadata.md)
- [organizer_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/organizer_from_mapi.md)
- [attendees_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/attendees_from_mapi.md)
- [serialize_calendar_participants_metadata](../../../../../../../functions/crates/lpe-storage/src/calendar/serialize_calendar_participants_metadata.md)
- [organizer_json_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/meeting/organizer_json_from_mapi.md)
- [calendar_attendee_labels](../../../../../../../functions/crates/lpe-storage/src/calendar/calendar_attendee_labels.md)

# Called by

- [event_input_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/event_input_from_mapi.md)
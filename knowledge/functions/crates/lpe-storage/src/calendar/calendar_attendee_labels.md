---
type: Rust Function
title: calendar_attendee_labels
resource: crates/lpe-storage/src/calendar.rs#L113-L121
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-dav/src/parse/parse_ical
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_display_to
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/meeting_response_event_input_from_mapi
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/event_participants_from_mapi
  - functions/crates/lpe-exchange/src/service/ews/calendar/parse_create_event_input
  - functions/crates/lpe-exchange/src/service/ews/calendar/parse_update_event_input
  - functions/crates/lpe-jmap/src/calendar/parse_calendar_participants
---

# Signature

`pub fn calendar_attendee_labels(metadata: &CalendarParticipantsMetadata) -> String`

# Called by

- [parse_ical](../../../../../functions/crates/lpe-dav/src/parse/parse_ical.md)
- [calendar_display_to](../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_display_to.md)
- [meeting_response_event_input_from_mapi](../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/meeting_response_event_input_from_mapi.md)
- [event_participants_from_mapi](../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/event_participants_from_mapi.md)
- [parse_create_event_input](../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/parse_create_event_input.md)
- [parse_update_event_input](../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/parse_update_event_input.md)
- [parse_calendar_participants](../../../../../functions/crates/lpe-jmap/src/calendar/parse_calendar_participants.md)
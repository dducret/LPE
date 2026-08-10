---
type: Rust Function
title: serialize_calendar_participants_metadata
resource: crates/lpe-storage/src/calendar.rs#L108-L111
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/calendar/normalize_calendar_participants_metadata
  called_by:
  - functions/crates/lpe-dav/src/parse/parse_ical
  - functions/crates/lpe-dav/src/tests/get_returns_not_modified_when_if_none_match_matches
  - functions/crates/lpe-dav/src/tests/get_serializes_organizer_and_participant_status
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/default_event_for_mapping
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/default_event_input
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/apply_calendar_pending_recipients
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/meeting_response_event_input_from_mapi
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/event_participants_from_mapi
  - functions/crates/lpe-exchange/src/mapi/properties/tests/calendar_projection_uses_canonical_all_day_status_and_participants
  - functions/crates/lpe-exchange/src/mapi/properties/tests/calendar_projection_does_not_mark_an_appointment_without_attendees_as_a_meeting
  - functions/crates/lpe-exchange/src/mapi/properties/tests/calendar_projection_keeps_meeting_state_after_all_attendees_are_removed
  - functions/crates/lpe-exchange/src/service/ews/calendar/parse_create_event_input
  - functions/crates/lpe-exchange/src/service/ews/calendar/parse_update_event_input
  - functions/crates/lpe-jmap/src/calendar/parse_calendar_participants_json
  - functions/crates/lpe-jmap/src/tests/FakeStore/event
  - functions/crates/lpe-jmap/src/tests/calendar_event_get_projects_mapi_written_canonical_fields
  - functions/crates/lpe-storage/src/calendar/serialized_calendar_participants_match_attendees_json_schema
---

# Signature

`pub fn serialize_calendar_participants_metadata(metadata: &CalendarParticipantsMetadata) -> String`

# Calls

- [normalize_calendar_participants_metadata](../../../../../functions/crates/lpe-storage/src/calendar/normalize_calendar_participants_metadata.md)

# Called by

- [parse_ical](../../../../../functions/crates/lpe-dav/src/parse/parse_ical.md)
- [get_returns_not_modified_when_if_none_match_matches](../../../../../functions/crates/lpe-dav/src/tests/get_returns_not_modified_when_if_none_match_matches.md)
- [get_serializes_organizer_and_participant_status](../../../../../functions/crates/lpe-dav/src/tests/get_serializes_organizer_and_participant_status.md)
- [default_event_for_mapping](../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/default_event_for_mapping.md)
- [default_event_input](../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/default_event_input.md)
- [apply_calendar_pending_recipients](../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/apply_calendar_pending_recipients.md)
- [meeting_response_event_input_from_mapi](../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/meeting_response_event_input_from_mapi.md)
- [event_participants_from_mapi](../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/event_participants_from_mapi.md)
- [calendar_projection_uses_canonical_all_day_status_and_participants](../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/calendar_projection_uses_canonical_all_day_status_and_participants.md)
- [calendar_projection_does_not_mark_an_appointment_without_attendees_as_a_meeting](../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/calendar_projection_does_not_mark_an_appointment_without_attendees_as_a_meeting.md)
- [calendar_projection_keeps_meeting_state_after_all_attendees_are_removed](../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/calendar_projection_keeps_meeting_state_after_all_attendees_are_removed.md)
- [parse_create_event_input](../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/parse_create_event_input.md)
- [parse_update_event_input](../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/parse_update_event_input.md)
- [parse_calendar_participants_json](../../../../../functions/crates/lpe-jmap/src/calendar/parse_calendar_participants_json.md)
- [event](../../../../../functions/crates/lpe-jmap/src/tests/FakeStore/event.md)
- [calendar_event_get_projects_mapi_written_canonical_fields](../../../../../functions/crates/lpe-jmap/src/tests/calendar_event_get_projects_mapi_written_canonical_fields.md)
- [serialized_calendar_participants_match_attendees_json_schema](../../../../../functions/crates/lpe-storage/src/calendar/serialized_calendar_participants_match_attendees_json_schema.md)
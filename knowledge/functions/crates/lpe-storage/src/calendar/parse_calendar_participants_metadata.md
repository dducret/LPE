---
type: Rust Function
title: parse_calendar_participants_metadata
resource: crates/lpe-storage/src/calendar.rs#L26-L106
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/calendar/normalize_calendar_participants_metadata
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-activesync/src/snapshot/calendar_application_data
  - functions/crates/lpe-dav/src/serialize/serialize_ical
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_organizer
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_display_to
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_all_attendees
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_required_attendees
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_optional_attendees
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/meeting_response_event_input_from_mapi
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/event_participants_from_mapi
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/meeting/event_is_meeting
  - functions/crates/lpe-exchange/src/service/ews/calendar/ews_attendees_xml
  - functions/crates/lpe-jmap/src/calendar/participants_from_event
  - functions/crates/lpe-storage/src/calendar/parser_still_accepts_legacy_combined_participant_metadata
---

# Signature

`pub fn parse_calendar_participants_metadata(raw: &str) -> CalendarParticipantsMetadata`

# Calls

- [normalize_calendar_participants_metadata](../../../../../functions/crates/lpe-storage/src/calendar/normalize_calendar_participants_metadata.md)
- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [calendar_application_data](../../../../../functions/crates/lpe-activesync/src/snapshot/calendar_application_data.md)
- [serialize_ical](../../../../../functions/crates/lpe-dav/src/serialize/serialize_ical.md)
- [calendar_organizer](../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_organizer.md)
- [calendar_display_to](../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_display_to.md)
- [calendar_all_attendees](../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_all_attendees.md)
- [calendar_required_attendees](../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_required_attendees.md)
- [calendar_optional_attendees](../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_optional_attendees.md)
- [meeting_response_event_input_from_mapi](../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/meeting_response_event_input_from_mapi.md)
- [event_participants_from_mapi](../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/event_participants_from_mapi.md)
- [event_is_meeting](../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/meeting/event_is_meeting.md)
- [ews_attendees_xml](../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/ews_attendees_xml.md)
- [participants_from_event](../../../../../functions/crates/lpe-jmap/src/calendar/participants_from_event.md)
- [parser_still_accepts_legacy_combined_participant_metadata](../../../../../functions/crates/lpe-storage/src/calendar/parser_still_accepts_legacy_combined_participant_metadata.md)
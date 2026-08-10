---
type: Rust Function
title: organizer_json_from_mapi
resource: crates/lpe-exchange/src/mapi/properties/calendar/meeting.rs#L24-L60
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/meeting/event_is_meeting
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/apply_calendar_pending_recipients
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/event_participants_from_mapi
---

# Signature

`pub(super) fn organizer_json_from_mapi( existing: &AccessibleEvent, organizer: Option<&CalendarOrganizerMetadata>, has_attendees: bool, properties: &HashMap<u32, MapiValue>, ) -> String`

# Calls

- [event_is_meeting](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/meeting/event_is_meeting.md)
- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [apply_calendar_pending_recipients](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/apply_calendar_pending_recipients.md)
- [event_participants_from_mapi](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/event_participants_from_mapi.md)
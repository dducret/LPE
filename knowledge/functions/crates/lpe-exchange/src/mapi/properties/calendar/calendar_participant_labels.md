---
type: Rust Function
title: calendar_participant_labels
resource: crates/lpe-exchange/src/mapi/properties/calendar.rs#L305-L319
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_all_attendees
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_required_attendees
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_optional_attendees
---

# Signature

`fn calendar_participant_labels<'a>( participants: impl Iterator<Item = &'a CalendarParticipantMetadata>, ) -> String`

# Called by

- [calendar_all_attendees](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_all_attendees.md)
- [calendar_required_attendees](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_required_attendees.md)
- [calendar_optional_attendees](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_optional_attendees.md)
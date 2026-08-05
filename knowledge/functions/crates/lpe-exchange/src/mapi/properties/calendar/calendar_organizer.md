---
type: Rust Function
title: calendar_organizer
resource: crates/lpe-exchange/src/mapi/properties/calendar.rs#L158-L166
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/calendar/parse_calendar_participants_metadata
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_organizer_name
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_organizer_email
---

# Signature

`fn calendar_organizer(event: &AccessibleEvent) -> CalendarOrganizerMetadata`

# Calls

- [parse_calendar_participants_metadata](../../../../../../../functions/crates/lpe-storage/src/calendar/parse_calendar_participants_metadata.md)

# Called by

- [calendar_organizer_name](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_organizer_name.md)
- [calendar_organizer_email](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_organizer_email.md)
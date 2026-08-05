---
type: Rust Function
title: calendar_participants_from_display_string
resource: crates/lpe-exchange/src/mapi/properties/calendar.rs#L867-L887
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/attendees_from_mapi
---

# Signature

`fn calendar_participants_from_display_string( value: &str, role: &str, ) -> Vec<CalendarParticipantMetadata>`

# Called by

- [attendees_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/attendees_from_mapi.md)
---
type: Rust Function
title: attendees_from_mapi
resource: crates/lpe-exchange/src/mapi/properties/calendar.rs#L880-L910
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/streams/pending_text_property
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_participants_from_display_string
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/event_participants_from_mapi
---

# Signature

`fn attendees_from_mapi( properties: &HashMap<u32, MapiValue>, ) -> Option<Vec<CalendarParticipantMetadata>>`

# Calls

- [pending_text_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/pending_text_property.md)
- [calendar_participants_from_display_string](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_participants_from_display_string.md)

# Called by

- [event_participants_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/event_participants_from_mapi.md)
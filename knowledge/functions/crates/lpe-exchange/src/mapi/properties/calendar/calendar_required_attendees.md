---
type: Rust Function
title: calendar_required_attendees
resource: crates/lpe-exchange/src/mapi/properties/calendar.rs#L196-L204
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/calendar/parse_calendar_participants_metadata
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_participant_labels
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/event_property_value_with_optional_version
---

# Signature

`fn calendar_required_attendees(event: &AccessibleEvent) -> String`

# Calls

- [parse_calendar_participants_metadata](../../../../../../../functions/crates/lpe-storage/src/calendar/parse_calendar_participants_metadata.md)
- [calendar_participant_labels](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_participant_labels.md)

# Called by

- [event_property_value_with_optional_version](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/event_property_value_with_optional_version.md)
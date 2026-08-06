---
type: Rust Function
title: calendar_display_to
resource: crates/lpe-exchange/src/mapi/properties/calendar.rs#L195-L203
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/calendar/parse_calendar_participants_metadata
  - functions/crates/lpe-storage/src/calendar/calendar_attendee_labels
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/event_property_value_with_optional_version
---

# Signature

`fn calendar_display_to(event: &AccessibleEvent) -> String`

# Calls

- [parse_calendar_participants_metadata](../../../../../../../functions/crates/lpe-storage/src/calendar/parse_calendar_participants_metadata.md)
- [calendar_attendee_labels](../../../../../../../functions/crates/lpe-storage/src/calendar/calendar_attendee_labels.md)

# Called by

- [event_property_value_with_optional_version](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/event_property_value_with_optional_version.md)
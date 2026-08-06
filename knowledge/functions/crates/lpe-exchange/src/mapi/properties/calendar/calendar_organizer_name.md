---
type: Rust Function
title: calendar_organizer_name
resource: crates/lpe-exchange/src/mapi/properties/calendar.rs#L182-L189
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_organizer
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/event_property_value_with_optional_version
---

# Signature

`fn calendar_organizer_name(event: &AccessibleEvent) -> String`

# Calls

- [calendar_organizer](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_organizer.md)

# Called by

- [event_property_value_with_optional_version](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/event_property_value_with_optional_version.md)
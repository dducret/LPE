---
type: Rust Function
title: calendar_time_zone_struct
resource: crates/lpe-exchange/src/mapi/properties/calendar.rs#L364-L375
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_time_zone
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/push_system_time
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/event_property_value_with_optional_version
---

# Signature

`fn calendar_time_zone_struct(event: &AccessibleEvent) -> Vec<u8>`

# Calls

- [calendar_time_zone](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_time_zone.md)
- [push_system_time](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/push_system_time.md)

# Called by

- [event_property_value_with_optional_version](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/event_property_value_with_optional_version.md)
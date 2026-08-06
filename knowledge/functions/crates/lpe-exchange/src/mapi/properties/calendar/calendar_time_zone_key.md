---
type: Rust Function
title: calendar_time_zone_key
resource: crates/lpe-exchange/src/mapi/properties/calendar.rs#L275-L277
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/recognized_calendar_time_zone_key
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/event_property_value_with_optional_version
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_time_zone_definition
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_time_zone
---

# Signature

`fn calendar_time_zone_key(time_zone: &str) -> &'static str`

# Calls

- [recognized_calendar_time_zone_key](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/recognized_calendar_time_zone_key.md)

# Called by

- [event_property_value_with_optional_version](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/event_property_value_with_optional_version.md)
- [calendar_time_zone_definition](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_time_zone_definition.md)
- [calendar_time_zone](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_time_zone.md)
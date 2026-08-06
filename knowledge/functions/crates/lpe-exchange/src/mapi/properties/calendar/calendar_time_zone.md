---
type: Rust Function
title: calendar_time_zone
resource: crates/lpe-exchange/src/mapi/properties/calendar.rs#L344-L376
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_time_zone_key
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/CalendarSystemTime/zero
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_time_zone_struct
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_time_zone_definition
---

# Signature

`fn calendar_time_zone(event: &AccessibleEvent) -> CalendarTimeZone`

# Calls

- [calendar_time_zone_key](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_time_zone_key.md)
- [zero](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/CalendarSystemTime/zero.md)

# Called by

- [calendar_time_zone_struct](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_time_zone_struct.md)
- [calendar_time_zone_definition](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_time_zone_definition.md)
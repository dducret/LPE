---
type: Rust Function
title: push_system_time
resource: crates/lpe-exchange/src/mapi/properties/calendar.rs#L391-L400
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_time_zone_struct
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/push_time_zone_rule
---

# Signature

`fn push_system_time(value: &mut Vec<u8>, system_time: CalendarSystemTime)`

# Called by

- [calendar_time_zone_struct](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_time_zone_struct.md)
- [push_time_zone_rule](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/push_time_zone_rule.md)
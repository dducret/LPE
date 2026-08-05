---
type: Rust Function
title: push_time_zone_rule
resource: crates/lpe-exchange/src/mapi/properties/calendar.rs#L377-L389
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/push_system_time
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_time_zone_definition
---

# Signature

`fn push_time_zone_rule(value: &mut Vec<u8>, tz: CalendarTimeZone)`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [push_system_time](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/push_system_time.md)

# Called by

- [calendar_time_zone_definition](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_time_zone_definition.md)
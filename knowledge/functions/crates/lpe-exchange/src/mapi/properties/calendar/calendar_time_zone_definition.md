---
type: Rust Function
title: calendar_time_zone_definition
resource: crates/lpe-exchange/src/mapi/properties/calendar.rs#L377-L398
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_time_zone
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_time_zone_key
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/push_time_zone_rule
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/event_property_value_with_optional_version
---

# Signature

`fn calendar_time_zone_definition(event: &AccessibleEvent) -> Vec<u8>`

# Calls

- [calendar_time_zone](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_time_zone.md)
- [calendar_time_zone_key](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_time_zone_key.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [push_time_zone_rule](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/push_time_zone_rule.md)

# Called by

- [event_property_value_with_optional_version](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/event_property_value_with_optional_version.md)
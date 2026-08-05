---
type: Rust Function
title: canonical_calendar_time_zone_key
resource: crates/lpe-exchange/src/mapi/properties/calendar.rs#L265-L273
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/recognized_calendar_time_zone_key
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_time_zone_from_mapi
---

# Signature

`fn canonical_calendar_time_zone_key(time_zone: &str) -> Option<&'static str>`

# Calls

- [recognized_calendar_time_zone_key](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/recognized_calendar_time_zone_key.md)

# Called by

- [calendar_time_zone_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_time_zone_from_mapi.md)
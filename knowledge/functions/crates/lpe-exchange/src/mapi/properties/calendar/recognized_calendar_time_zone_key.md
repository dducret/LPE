---
type: Rust Function
title: recognized_calendar_time_zone_key
resource: crates/lpe-exchange/src/mapi/properties/calendar.rs#L251-L259
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/time/is_western_europe_calendar_time_zone
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_time_zone_key
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/canonical_calendar_time_zone_key
---

# Signature

`fn recognized_calendar_time_zone_key(time_zone: &str) -> Option<&'static str>`

# Calls

- [is_western_europe_calendar_time_zone](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/is_western_europe_calendar_time_zone.md)

# Called by

- [calendar_time_zone_key](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_time_zone_key.md)
- [canonical_calendar_time_zone_key](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/canonical_calendar_time_zone_key.md)
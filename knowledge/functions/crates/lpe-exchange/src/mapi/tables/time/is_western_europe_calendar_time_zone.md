---
type: Rust Function
title: is_western_europe_calendar_time_zone
resource: crates/lpe-exchange/src/mapi/tables/time.rs#L88-L96
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/recognized_calendar_time_zone_key
  - functions/crates/lpe-exchange/src/mapi/tables/time/date_time_to_filetime_in_time_zone
  - functions/crates/lpe-exchange/src/mapi/tables/time/filetime_to_date_time_in_time_zone
---

# Signature

`pub(in crate::mapi) fn is_western_europe_calendar_time_zone(time_zone: &str) -> bool`

# Called by

- [recognized_calendar_time_zone_key](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/recognized_calendar_time_zone_key.md)
- [date_time_to_filetime_in_time_zone](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/date_time_to_filetime_in_time_zone.md)
- [filetime_to_date_time_in_time_zone](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/filetime_to_date_time_in_time_zone.md)
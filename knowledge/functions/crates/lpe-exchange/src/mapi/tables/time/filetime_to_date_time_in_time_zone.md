---
type: Rust Function
title: filetime_to_date_time_in_time_zone
resource: crates/lpe-exchange/src/mapi/tables/time.rs#L72-L86
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
  - functions/crates/lpe-exchange/src/mapi/tables/time/filetime_to_unix_seconds
  - functions/crates/lpe-exchange/src/mapi/tables/time/is_western_europe_calendar_time_zone
  - functions/crates/lpe-exchange/src/mapi/tables/time/western_europe_utc_offset_seconds
  - functions/crates/lpe-exchange/src/mapi/tables/time/unix_seconds_to_date_time
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/event_input_from_mapi
---

# Signature

`pub(in crate::mapi) fn filetime_to_date_time_in_time_zone( filetime: i64, time_zone: &str, ) -> Option<(String, String)>`

# Calls

- [try_from](../../../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)
- [filetime_to_unix_seconds](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/filetime_to_unix_seconds.md)
- [is_western_europe_calendar_time_zone](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/is_western_europe_calendar_time_zone.md)
- [western_europe_utc_offset_seconds](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/western_europe_utc_offset_seconds.md)
- [unix_seconds_to_date_time](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/unix_seconds_to_date_time.md)

# Called by

- [event_input_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/event_input_from_mapi.md)
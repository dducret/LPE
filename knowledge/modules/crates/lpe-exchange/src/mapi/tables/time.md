---
type: Rust Module
title: time
resource: crates/lpe-exchange/src/mapi/tables/time.rs#L1-L151
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super
  - external/lpe-domain-days-from-civil-unix-seconds-from-windows-filetime-windows-filetime-from-unix-seconds-windows-filetime-ticks-per-second
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [event_start_filetime](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/event_start_filetime.md)
- [event_end_filetime](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/event_end_filetime.md)
- [date_time_to_filetime](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/date_time_to_filetime.md)
- [filetime_to_date_time](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/filetime_to_date_time.md)
- [date_time_to_filetime_in_time_zone](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/date_time_to_filetime_in_time_zone.md)
- [filetime_to_date_time_in_time_zone](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/filetime_to_date_time_in_time_zone.md)
- [is_western_europe_calendar_time_zone](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/is_western_europe_calendar_time_zone.md)
- [unix_seconds_to_date_time](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/unix_seconds_to_date_time.md)
- [western_europe_local_utc_offset_seconds](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/western_europe_local_utc_offset_seconds.md)
- [western_europe_utc_offset_seconds](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/western_europe_utc_offset_seconds.md)
- [western_europe_transition_seconds](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/western_europe_transition_seconds.md)
- [unix_seconds_to_filetime](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/unix_seconds_to_filetime.md)
- [filetime_to_unix_seconds](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/filetime_to_unix_seconds.md)

# Imports

- `super::*`
- `lpe_domain::{
    days_from_civil, unix_seconds_from_windows_filetime, windows_filetime_from_unix_seconds,
    WINDOWS_FILETIME_TICKS_PER_SECOND,
}`

# Member of

- [lpe-exchange](../../../../../../packages/crates/lpe-exchange.md)
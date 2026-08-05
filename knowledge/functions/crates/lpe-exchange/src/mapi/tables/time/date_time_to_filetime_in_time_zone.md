---
type: Rust Function
title: date_time_to_filetime_in_time_zone
resource: crates/lpe-exchange/src/mapi/tables/time.rs#L52-L70
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/time/date_time_to_filetime
  - functions/crates/lpe-exchange/src/mapi/tables/time/is_western_europe_calendar_time_zone
  - functions/crates/lpe-exchange/src/mapi/tables/time/filetime_to_unix_seconds
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/tables/time/western_europe_local_utc_offset_seconds
  - functions/crates/lpe-exchange/src/mapi/tables/time/unix_seconds_to_filetime
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/time/event_start_filetime
---

# Signature

`pub(in crate::mapi) fn date_time_to_filetime_in_time_zone( date: &str, time: &str, time_zone: &str, ) -> u64`

# Calls

- [date_time_to_filetime](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/date_time_to_filetime.md)
- [is_western_europe_calendar_time_zone](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/is_western_europe_calendar_time_zone.md)
- [filetime_to_unix_seconds](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/filetime_to_unix_seconds.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [western_europe_local_utc_offset_seconds](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/western_europe_local_utc_offset_seconds.md)
- [unix_seconds_to_filetime](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/unix_seconds_to_filetime.md)

# Called by

- [event_start_filetime](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/event_start_filetime.md)
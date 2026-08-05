---
type: Rust Module
title: civil_time
resource: crates/lpe-domain/src/civil_time.rs#L1-L151
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/std-time-systemtime-unix-epoch
  - external/super
  member_of:
  - packages/crates/lpe-domain
---

# Contains

- [UtcDateTime](../../../../classes/crates/lpe-domain/src/civil_time/UtcDateTime.md)
- [days_from_civil](../../../../functions/crates/lpe-domain/src/civil_time/days_from_civil.md)
- [civil_from_days](../../../../functions/crates/lpe-domain/src/civil_time/civil_from_days.md)
- [utc_from_unix_seconds](../../../../functions/crates/lpe-domain/src/civil_time/utc_from_unix_seconds.md)
- [windows_filetime_from_unix_seconds](../../../../functions/crates/lpe-domain/src/civil_time/windows_filetime_from_unix_seconds.md)
- [windows_filetime_from_signed_unix_seconds](../../../../functions/crates/lpe-domain/src/civil_time/windows_filetime_from_signed_unix_seconds.md)
- [unix_seconds_from_windows_filetime](../../../../functions/crates/lpe-domain/src/civil_time/unix_seconds_from_windows_filetime.md)
- [current_windows_filetime](../../../../functions/crates/lpe-domain/src/civil_time/current_windows_filetime.md)
- [weekday_abbrev_from_unix_days](../../../../functions/crates/lpe-domain/src/civil_time/weekday_abbrev_from_unix_days.md)
- [month_abbrev](../../../../functions/crates/lpe-domain/src/civil_time/month_abbrev.md)
- [civil_round_trip_handles_epoch_and_leap_day](../../../../functions/crates/lpe-domain/src/civil_time/civil_round_trip_handles_epoch_and_leap_day.md)
- [utc_parts_include_weekday_and_month_names](../../../../functions/crates/lpe-domain/src/civil_time/utc_parts_include_weekday_and_month_names.md)
- [windows_filetime_round_trips_unix_seconds](../../../../functions/crates/lpe-domain/src/civil_time/windows_filetime_round_trips_unix_seconds.md)

# Imports

- `std::time::{SystemTime, UNIX_EPOCH}`
- `super::*`

# Member of

- [lpe-domain](../../../../packages/crates/lpe-domain.md)
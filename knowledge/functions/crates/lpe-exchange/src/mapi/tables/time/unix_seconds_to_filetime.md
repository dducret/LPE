---
type: Rust Function
title: unix_seconds_to_filetime
resource: crates/lpe-exchange/src/mapi/tables/time.rs#L145-L147
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-domain/src/civil_time/windows_filetime_from_unix_seconds
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/time/date_time_to_filetime
  - functions/crates/lpe-exchange/src/mapi/tables/time/date_time_to_filetime_in_time_zone
---

# Signature

`pub(in crate::mapi) fn unix_seconds_to_filetime(unix_seconds: u64) -> u64`

# Calls

- [windows_filetime_from_unix_seconds](../../../../../../../functions/crates/lpe-domain/src/civil_time/windows_filetime_from_unix_seconds.md)

# Called by

- [date_time_to_filetime](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/date_time_to_filetime.md)
- [date_time_to_filetime_in_time_zone](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/date_time_to_filetime_in_time_zone.md)
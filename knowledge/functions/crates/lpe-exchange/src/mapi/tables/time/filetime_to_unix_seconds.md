---
type: Rust Function
title: filetime_to_unix_seconds
resource: crates/lpe-exchange/src/mapi/tables/time.rs#L149-L151
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-domain/src/civil_time/unix_seconds_from_windows_filetime
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/time/filetime_to_date_time
  - functions/crates/lpe-exchange/src/mapi/tables/time/date_time_to_filetime_in_time_zone
  - functions/crates/lpe-exchange/src/mapi/tables/time/filetime_to_date_time_in_time_zone
---

# Signature

`pub(in crate::mapi) fn filetime_to_unix_seconds(filetime: u64) -> Option<u64>`

# Calls

- [unix_seconds_from_windows_filetime](../../../../../../../functions/crates/lpe-domain/src/civil_time/unix_seconds_from_windows_filetime.md)

# Called by

- [filetime_to_date_time](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/filetime_to_date_time.md)
- [date_time_to_filetime_in_time_zone](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/date_time_to_filetime_in_time_zone.md)
- [filetime_to_date_time_in_time_zone](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/filetime_to_date_time_in_time_zone.md)
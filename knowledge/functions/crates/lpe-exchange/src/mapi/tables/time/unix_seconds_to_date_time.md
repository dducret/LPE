---
type: Rust Function
title: unix_seconds_to_date_time
resource: crates/lpe-exchange/src/mapi/tables/time.rs#L98-L108
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/logon/civil_from_unix_days
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/time/filetime_to_date_time
  - functions/crates/lpe-exchange/src/mapi/tables/time/filetime_to_date_time_in_time_zone
---

# Signature

`fn unix_seconds_to_date_time(unix_seconds: u64) -> (String, String)`

# Calls

- [civil_from_unix_days](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/logon/civil_from_unix_days.md)

# Called by

- [filetime_to_date_time](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/filetime_to_date_time.md)
- [filetime_to_date_time_in_time_zone](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/filetime_to_date_time_in_time_zone.md)
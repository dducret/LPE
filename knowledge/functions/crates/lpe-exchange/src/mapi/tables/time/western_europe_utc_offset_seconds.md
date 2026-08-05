---
type: Rust Function
title: western_europe_utc_offset_seconds
resource: crates/lpe-exchange/src/mapi/tables/time.rs#L124-L136
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/logon/civil_from_unix_days
  - functions/crates/lpe-exchange/src/mapi/tables/time/western_europe_transition_seconds
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/time/filetime_to_date_time_in_time_zone
---

# Signature

`fn western_europe_utc_offset_seconds(unix_seconds: u64) -> u64`

# Calls

- [civil_from_unix_days](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/logon/civil_from_unix_days.md)
- [western_europe_transition_seconds](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/western_europe_transition_seconds.md)

# Called by

- [filetime_to_date_time_in_time_zone](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/filetime_to_date_time_in_time_zone.md)
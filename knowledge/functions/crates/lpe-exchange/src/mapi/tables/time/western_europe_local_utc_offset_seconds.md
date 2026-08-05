---
type: Rust Function
title: western_europe_local_utc_offset_seconds
resource: crates/lpe-exchange/src/mapi/tables/time.rs#L110-L122
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/time/western_europe_transition_seconds
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/time/date_time_to_filetime_in_time_zone
---

# Signature

`fn western_europe_local_utc_offset_seconds(year: i32, local_unix_seconds: u64) -> u64`

# Calls

- [western_europe_transition_seconds](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/western_europe_transition_seconds.md)

# Called by

- [date_time_to_filetime_in_time_zone](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/date_time_to_filetime_in_time_zone.md)
---
type: Rust Function
title: civil_from_unix_days
resource: crates/lpe-exchange/src/mapi/rop/logon.rs#L99-L111
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/logon/logon_time_bytes
  - functions/crates/lpe-exchange/src/mapi/tables/time/unix_seconds_to_date_time
  - functions/crates/lpe-exchange/src/mapi/tables/time/western_europe_utc_offset_seconds
---

# Signature

`pub(in crate::mapi) fn civil_from_unix_days(days: i64) -> (i32, u8, u8)`

# Called by

- [logon_time_bytes](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/logon/logon_time_bytes.md)
- [unix_seconds_to_date_time](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/unix_seconds_to_date_time.md)
- [western_europe_utc_offset_seconds](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/western_europe_utc_offset_seconds.md)
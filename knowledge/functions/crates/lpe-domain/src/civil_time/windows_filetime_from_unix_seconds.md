---
type: Rust Function
title: windows_filetime_from_unix_seconds
resource: crates/lpe-domain/src/civil_time.rs#L56-L60
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-domain/src/civil_time/windows_filetime_round_trips_unix_seconds
  - functions/crates/lpe-exchange/src/mapi/tables/time/unix_seconds_to_filetime
---

# Signature

`pub fn windows_filetime_from_unix_seconds(unix_seconds: u64) -> u64`

# Called by

- [windows_filetime_round_trips_unix_seconds](../../../../../functions/crates/lpe-domain/src/civil_time/windows_filetime_round_trips_unix_seconds.md)
- [unix_seconds_to_filetime](../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/unix_seconds_to_filetime.md)
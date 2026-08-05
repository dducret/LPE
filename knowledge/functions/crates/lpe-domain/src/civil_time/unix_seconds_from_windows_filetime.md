---
type: Rust Function
title: unix_seconds_from_windows_filetime
resource: crates/lpe-domain/src/civil_time.rs#L69-L73
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/time/filetime_to_unix_seconds
---

# Signature

`pub fn unix_seconds_from_windows_filetime(filetime: u64) -> Option<u64>`

# Called by

- [filetime_to_unix_seconds](../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/filetime_to_unix_seconds.md)
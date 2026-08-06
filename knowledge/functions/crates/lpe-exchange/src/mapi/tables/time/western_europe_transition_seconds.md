---
type: Rust Function
title: western_europe_transition_seconds
resource: crates/lpe-exchange/src/mapi/tables/time.rs#L138-L143
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/time/western_europe_local_utc_offset_seconds
  - functions/crates/lpe-exchange/src/mapi/tables/time/western_europe_utc_offset_seconds
---

# Signature

`fn western_europe_transition_seconds(year: i32, month: u32, hour: u64) -> u64`

# Called by

- [western_europe_local_utc_offset_seconds](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/western_europe_local_utc_offset_seconds.md)
- [western_europe_utc_offset_seconds](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/western_europe_utc_offset_seconds.md)
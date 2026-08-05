---
type: Rust Function
title: parse_time
resource: crates/lpe-activesync/src/snapshot.rs#L362-L367
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  called_by:
  - functions/crates/lpe-activesync/src/snapshot/add_minutes_to_compact
---

# Signature

`fn parse_time(value: &str) -> Option<(i64, i64)>`

# Calls

- [next](../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)

# Called by

- [add_minutes_to_compact](../../../../../functions/crates/lpe-activesync/src/snapshot/add_minutes_to_compact.md)
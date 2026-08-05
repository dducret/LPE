---
type: Rust Function
title: parse_date
resource: crates/lpe-activesync/src/snapshot.rs#L351-L360
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

`fn parse_date(value: &str) -> Option<(i64, i64, i64)>`

# Calls

- [next](../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)

# Called by

- [add_minutes_to_compact](../../../../../functions/crates/lpe-activesync/src/snapshot/add_minutes_to_compact.md)
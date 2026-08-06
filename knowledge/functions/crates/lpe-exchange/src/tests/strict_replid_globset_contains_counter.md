---
type: Rust Function
title: strict_replid_globset_contains_counter
resource: crates/lpe-exchange/src/tests/mod.rs#L14338-L14343
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/strict_globcnt_to_u64
  - functions/crates/lpe-exchange/src/tests/strict_replid_globset_ranges
---

# Signature

`fn strict_replid_globset_contains_counter(value: &[u8], counter: &[u8]) -> Result<bool, String>`

# Calls

- [strict_globcnt_to_u64](../../../../../functions/crates/lpe-exchange/src/tests/strict_globcnt_to_u64.md)
- [strict_replid_globset_ranges](../../../../../functions/crates/lpe-exchange/src/tests/strict_replid_globset_ranges.md)
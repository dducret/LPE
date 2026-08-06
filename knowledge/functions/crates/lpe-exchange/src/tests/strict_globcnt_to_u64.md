---
type: Rust Function
title: strict_globcnt_to_u64
resource: crates/lpe-exchange/src/tests/mod.rs#L13814-L13821
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/strict_replguid_globset_contains_counter
  - functions/crates/lpe-exchange/src/tests/strict_replguid_globset_ranges
  - functions/crates/lpe-exchange/src/tests/strict_replid_globset_contains_counter
  - functions/crates/lpe-exchange/src/tests/strict_replid_globset_ranges
---

# Signature

`fn strict_globcnt_to_u64(bytes: &[u8]) -> Result<u64, String>`

# Called by

- [strict_replguid_globset_contains_counter](../../../../../functions/crates/lpe-exchange/src/tests/strict_replguid_globset_contains_counter.md)
- [strict_replguid_globset_ranges](../../../../../functions/crates/lpe-exchange/src/tests/strict_replguid_globset_ranges.md)
- [strict_replid_globset_contains_counter](../../../../../functions/crates/lpe-exchange/src/tests/strict_replid_globset_contains_counter.md)
- [strict_replid_globset_ranges](../../../../../functions/crates/lpe-exchange/src/tests/strict_replid_globset_ranges.md)
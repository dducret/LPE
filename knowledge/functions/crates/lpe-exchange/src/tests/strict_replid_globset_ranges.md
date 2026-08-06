---
type: Rust Function
title: strict_replid_globset_ranges
resource: crates/lpe-exchange/src/tests/mod.rs#L14386-L14488
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/tests/read_strict_slice
  - functions/crates/lpe-exchange/src/tests/strict_globcnt_to_u64
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/tests/strict_validate_replid_globset
  - functions/crates/lpe-exchange/src/tests/strict_replid_globset_contains_counter
---

# Signature

`fn strict_replid_globset_ranges(value: &[u8]) -> Result<Vec<(u64, u64)>, String>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [read_strict_slice](../../../../../functions/crates/lpe-exchange/src/tests/read_strict_slice.md)
- [strict_globcnt_to_u64](../../../../../functions/crates/lpe-exchange/src/tests/strict_globcnt_to_u64.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [strict_validate_replid_globset](../../../../../functions/crates/lpe-exchange/src/tests/strict_validate_replid_globset.md)
- [strict_replid_globset_contains_counter](../../../../../functions/crates/lpe-exchange/src/tests/strict_replid_globset_contains_counter.md)
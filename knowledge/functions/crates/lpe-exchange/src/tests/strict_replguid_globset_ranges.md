---
type: Rust Function
title: strict_replguid_globset_ranges
resource: crates/lpe-exchange/src/tests/mod.rs#L13784-L13818
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/tests/strict_globcnt_to_u64
  - functions/crates/lpe-exchange/src/tests/read_strict_slice
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/tests/strict_validate_replguid_globset
  - functions/crates/lpe-exchange/src/tests/strict_replguid_globset_contains_counter
---

# Signature

`fn strict_replguid_globset_ranges(value: &[u8]) -> Result<Vec<(u64, u64)>, String>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [strict_globcnt_to_u64](../../../../../functions/crates/lpe-exchange/src/tests/strict_globcnt_to_u64.md)
- [read_strict_slice](../../../../../functions/crates/lpe-exchange/src/tests/read_strict_slice.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [strict_validate_replguid_globset](../../../../../functions/crates/lpe-exchange/src/tests/strict_validate_replguid_globset.md)
- [strict_replguid_globset_contains_counter](../../../../../functions/crates/lpe-exchange/src/tests/strict_replguid_globset_contains_counter.md)
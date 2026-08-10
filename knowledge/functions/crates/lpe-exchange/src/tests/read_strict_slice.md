---
type: Rust Function
title: read_strict_slice
resource: crates/lpe-exchange/src/tests/mod.rs#L13839-L13843
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/tests/strict_parse_fast_transfer_property
  - functions/crates/lpe-exchange/src/tests/strict_replguid_globset_ranges
  - functions/crates/lpe-exchange/src/tests/read_strict_u32
  - functions/crates/lpe-exchange/src/tests/strict_replid_globset_ranges
---

# Signature

`fn read_strict_slice(bytes: &[u8], offset: usize, len: usize) -> Result<&[u8], String>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [strict_parse_fast_transfer_property](../../../../../functions/crates/lpe-exchange/src/tests/strict_parse_fast_transfer_property.md)
- [strict_replguid_globset_ranges](../../../../../functions/crates/lpe-exchange/src/tests/strict_replguid_globset_ranges.md)
- [read_strict_u32](../../../../../functions/crates/lpe-exchange/src/tests/read_strict_u32.md)
- [strict_replid_globset_ranges](../../../../../functions/crates/lpe-exchange/src/tests/strict_replid_globset_ranges.md)
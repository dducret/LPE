---
type: Rust Function
title: read_debug_slice
resource: crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec.rs#L1031-L1035
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/parse_debug_fast_transfer_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/read_debug_u32
---

# Signature

`pub(super) fn read_debug_slice(bytes: &[u8], offset: usize, len: usize) -> Result<&[u8], String>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [parse_debug_fast_transfer_property](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/parse_debug_fast_transfer_property.md)
- [read_debug_u32](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/codec/read_debug_u32.md)
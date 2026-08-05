---
type: Rust Function
title: write_multi_i16
resource: crates/lpe-exchange/src/mapi/properties/values.rs#L395-L400
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u16
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value
---

# Signature

`pub(in crate::mapi) fn write_multi_i16(row: &mut Vec<u8>, values: &[i16])`

# Calls

- [write_u16](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u16.md)

# Called by

- [write_mapi_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value.md)
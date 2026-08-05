---
type: Rust Function
title: write_multi_string
resource: crates/lpe-exchange/src/mapi/properties/values.rs#L423-L428
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_utf16z
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value
---

# Signature

`pub(in crate::mapi) fn write_multi_string(row: &mut Vec<u8>, values: &[String])`

# Calls

- [write_utf16z](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_utf16z.md)

# Called by

- [write_mapi_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value.md)
---
type: Rust Function
title: write_multi_string8
resource: crates/lpe-exchange/src/mapi/properties/values.rs#L416-L421
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_ascii_z
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value
---

# Signature

`pub(in crate::mapi) fn write_multi_string8(row: &mut Vec<u8>, values: &[String])`

# Calls

- [write_ascii_z](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_ascii_z.md)

# Called by

- [write_mapi_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value.md)
---
type: Rust Function
title: write_multi_binary
resource: crates/lpe-exchange/src/mapi/properties/values.rs#L437-L442
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_rop_binary
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value
---

# Signature

`pub(in crate::mapi) fn write_multi_binary(row: &mut Vec<u8>, values: &[Vec<u8>])`

# Calls

- [write_rop_binary](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_rop_binary.md)

# Called by

- [write_mapi_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value.md)
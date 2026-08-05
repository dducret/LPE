---
type: Rust Function
title: write_multi_guid
resource: crates/lpe-exchange/src/mapi/properties/values.rs#L430-L435
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value
---

# Signature

`pub(in crate::mapi) fn write_multi_guid(row: &mut Vec<u8>, values: &[[u8; 16]])`

# Called by

- [write_mapi_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value.md)
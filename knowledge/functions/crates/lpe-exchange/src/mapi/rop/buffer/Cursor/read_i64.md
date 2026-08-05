---
type: Rust Method
title: read_i64
resource: crates/lpe-exchange/src/mapi/rop/buffer.rs#L23-L28
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_bytes
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/read_fast_transfer_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/values/parse_mapi_property_value
---

# Signature

`pub(in crate::mapi) fn read_i64(&mut self) -> Result<i64>`

# Calls

- [read_bytes](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_bytes.md)

# Called by

- [read_fast_transfer_property_value](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/read_fast_transfer_property_value.md)
- [parse_mapi_property_value](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/parse_mapi_property_value.md)
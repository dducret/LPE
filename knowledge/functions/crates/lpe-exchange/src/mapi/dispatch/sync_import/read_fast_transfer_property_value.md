---
type: Rust Function
title: read_fast_transfer_property_value
resource: crates/lpe-exchange/src/mapi/dispatch/sync_import.rs#L638-L669
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u16
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_i32
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_i64
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/read_fast_transfer_variable_bytes
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/decode_fast_transfer_string8
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/decode_fast_transfer_utf16
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_bytes
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/fast_transfer_property_values
---

# Signature

`fn read_fast_transfer_property_value( cursor: &mut Cursor<'_>, property_tag: u32, ) -> Result<MapiValue>`

# Calls

- [read_u16](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u16.md)
- [read_i32](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_i32.md)
- [read_i64](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_i64.md)
- [read_fast_transfer_variable_bytes](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/read_fast_transfer_variable_bytes.md)
- [decode_fast_transfer_string8](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/decode_fast_transfer_string8.md)
- [decode_fast_transfer_utf16](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/decode_fast_transfer_utf16.md)
- [read_bytes](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_bytes.md)

# Called by

- [fast_transfer_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/fast_transfer_property_values.md)
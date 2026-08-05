---
type: Rust Function
title: parse_named_property
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L1458-L1474
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u8
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_bytes
  - functions/crates/lpe-exchange/src/mapi/rop/parse/decode_utf16z_bytes
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/named_property_names
---

# Signature

`pub(in crate::mapi) fn parse_named_property(cursor: &mut Cursor<'_>) -> Result<MapiNamedProperty>`

# Calls

- [read_u8](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u8.md)
- [read_bytes](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_bytes.md)
- [decode_utf16z_bytes](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/decode_utf16z_bytes.md)

# Called by

- [named_property_names](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/named_property_names.md)
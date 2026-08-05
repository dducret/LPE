---
type: Rust Function
title: parse_resolve_names_values
resource: crates/lpe-exchange/src/mapi/nspi.rs#L328-L360
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u8
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_bytes
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u16
  - functions/crates/lpe-exchange/src/mapi/nspi/decode_utf16le_string
  - functions/crates/lpe-exchange/src/mapi/nspi/normalize_nspi_lookup_value
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/resolve_names_requested_values
---

# Signature

`pub(in crate::mapi) fn parse_resolve_names_values(request: &[u8]) -> Option<Vec<String>>`

# Calls

- [read_u8](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u8.md)
- [read_bytes](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_bytes.md)
- [read_u16](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u16.md)
- [decode_utf16le_string](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/decode_utf16le_string.md)
- [normalize_nspi_lookup_value](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/normalize_nspi_lookup_value.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [resolve_names_requested_values](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/resolve_names_requested_values.md)
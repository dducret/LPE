---
type: Rust Function
title: parse_resolve_names_columns
resource: crates/lpe-exchange/src/mapi/nspi.rs#L303-L323
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u8
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_bytes
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/resolve_names_columns
---

# Signature

`pub(in crate::mapi) fn parse_resolve_names_columns(request: &[u8]) -> Option<Vec<u32>>`

# Calls

- [read_u8](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u8.md)
- [read_bytes](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_bytes.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [resolve_names_columns](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/resolve_names_columns.md)
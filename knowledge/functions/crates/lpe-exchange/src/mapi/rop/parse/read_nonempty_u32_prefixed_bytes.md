---
type: Rust Function
title: read_nonempty_u32_prefixed_bytes
resource: crates/lpe-exchange/src/mapi/rop/parse.rs#L1305-L1308
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_bytes
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_move
---

# Signature

`fn read_nonempty_u32_prefixed_bytes<'a>(cursor: &mut Cursor<'a>) -> Option<&'a [u8]>`

# Calls

- [read_bytes](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_bytes.md)

# Called by

- [import_move](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_move.md)
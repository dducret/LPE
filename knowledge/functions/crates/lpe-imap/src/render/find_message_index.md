---
type: Rust Function
title: find_message_index
resource: crates/lpe-imap/src/render.rs#L1200-L1208
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position
  called_by:
  - functions/crates/lpe-imap/src/render/resolve_message_indexes
---

# Signature

`fn find_message_index(emails: &[ImapEmail], value: u32, ref_kind: MessageRefKind) -> Option<usize>`

# Calls

- [position](../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position.md)

# Called by

- [resolve_message_indexes](../../../../../functions/crates/lpe-imap/src/render/resolve_message_indexes.md)
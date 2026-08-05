---
type: Rust Function
title: command_tag_from_bytes
resource: crates/lpe-imap/src/service.rs#L156-L166
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/service/trim_line_end
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position
  called_by:
  - functions/crates/lpe-imap/src/service/ImapServer/handle_connection
---

# Signature

`fn command_tag_from_bytes(bytes: &[u8]) -> Option<&str>`

# Calls

- [trim_line_end](../../../../../functions/crates/lpe-imap/src/service/trim_line_end.md)
- [position](../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position.md)

# Called by

- [handle_connection](../../../../../functions/crates/lpe-imap/src/service/ImapServer/handle_connection.md)
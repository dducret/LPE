---
type: Rust Function
title: decode_command_line
resource: crates/lpe-imap/src/service.rs#L144-L147
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/service/trim_line_end
  called_by:
  - functions/crates/lpe-imap/src/service/ImapServer/handle_connection
  - functions/crates/lpe-imap/src/service/read_command_literals
---

# Signature

`fn decode_command_line(bytes: &[u8]) -> Result<&str>`

# Calls

- [trim_line_end](../../../../../functions/crates/lpe-imap/src/service/trim_line_end.md)

# Called by

- [handle_connection](../../../../../functions/crates/lpe-imap/src/service/ImapServer/handle_connection.md)
- [read_command_literals](../../../../../functions/crates/lpe-imap/src/service/read_command_literals.md)
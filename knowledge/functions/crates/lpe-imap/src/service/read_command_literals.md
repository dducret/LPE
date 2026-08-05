---
type: Rust Function
title: read_command_literals
resource: crates/lpe-imap/src/service.rs#L115-L142
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/service/trailing_literal
  - functions/crates/lpe-imap/src/service/decode_command_line
  called_by:
  - functions/crates/lpe-imap/src/service/ImapServer/handle_connection
---

# Signature

`async fn read_command_literals<R, W>( reader: &mut BufReader<R>, writer: &mut W, initial_line: &str, ) -> Result<String> where R: AsyncReadExt + Unpin, W: AsyncWriteExt + Unpin,`

# Calls

- [trailing_literal](../../../../../functions/crates/lpe-imap/src/service/trailing_literal.md)
- [decode_command_line](../../../../../functions/crates/lpe-imap/src/service/decode_command_line.md)

# Called by

- [handle_connection](../../../../../functions/crates/lpe-imap/src/service/ImapServer/handle_connection.md)
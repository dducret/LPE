---
type: Rust Method
title: handle_connection
resource: crates/lpe-imap/src/service.rs#L53-L112
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/service/decode_command_line
  - functions/crates/lpe-imap/src/service/command_tag_from_bytes
  - functions/crates/lpe-imap/src/service/read_command_literals
---

# Signature

`async fn handle_connection(&self, stream: TcpStream) -> Result<()>`

# Calls

- [decode_command_line](../../../../../../functions/crates/lpe-imap/src/service/decode_command_line.md)
- [command_tag_from_bytes](../../../../../../functions/crates/lpe-imap/src/service/command_tag_from_bytes.md)
- [read_command_literals](../../../../../../functions/crates/lpe-imap/src/service/read_command_literals.md)
---
type: Rust Function
title: send_raw_command
resource: crates/lpe-imap/src/tests.rs#L4025-L4029
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/tests/read_response
  called_by:
  - functions/crates/lpe-imap/src/tests/malformed_utf8_command_literals_are_rejected_before_mailbox_validation
  - functions/crates/lpe-imap/src/tests/malformed_utf8_quoted_mailbox_commands_are_rejected_cleanly
  - functions/crates/lpe-imap/src/tests/append_message_literals_remain_byte_oriented
---

# Signature

`async fn send_raw_command(stream: &mut TcpStream, command: &[u8], tag: &str) -> String`

# Calls

- [read_response](../../../../../functions/crates/lpe-imap/src/tests/read_response.md)

# Called by

- [malformed_utf8_command_literals_are_rejected_before_mailbox_validation](../../../../../functions/crates/lpe-imap/src/tests/malformed_utf8_command_literals_are_rejected_before_mailbox_validation.md)
- [malformed_utf8_quoted_mailbox_commands_are_rejected_cleanly](../../../../../functions/crates/lpe-imap/src/tests/malformed_utf8_quoted_mailbox_commands_are_rejected_cleanly.md)
- [append_message_literals_remain_byte_oriented](../../../../../functions/crates/lpe-imap/src/tests/append_message_literals_remain_byte_oriented.md)
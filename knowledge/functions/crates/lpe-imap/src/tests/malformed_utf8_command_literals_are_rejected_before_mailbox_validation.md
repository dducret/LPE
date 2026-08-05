---
type: Rust Function
title: malformed_utf8_command_literals_are_rejected_before_mailbox_validation
resource: crates/lpe-imap/src/tests.rs#L1568-L1592
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/service/ImapServer/with_validator
  - functions/crates/lpe-storage/src/core/Storage/connect
  - functions/crates/lpe-imap/src/tests/read_response
  - functions/crates/lpe-imap/src/tests/send_command
  - functions/crates/lpe-imap/src/tests/send_partial_command
  - functions/crates/lpe-imap/src/tests/send_raw_command
---

# Signature

`async fn malformed_utf8_command_literals_are_rejected_before_mailbox_validation()`

# Calls

- [with_validator](../../../../../functions/crates/lpe-imap/src/service/ImapServer/with_validator.md)
- [connect](../../../../../functions/crates/lpe-storage/src/core/Storage/connect.md)
- [read_response](../../../../../functions/crates/lpe-imap/src/tests/read_response.md)
- [send_command](../../../../../functions/crates/lpe-imap/src/tests/send_command.md)
- [send_partial_command](../../../../../functions/crates/lpe-imap/src/tests/send_partial_command.md)
- [send_raw_command](../../../../../functions/crates/lpe-imap/src/tests/send_raw_command.md)
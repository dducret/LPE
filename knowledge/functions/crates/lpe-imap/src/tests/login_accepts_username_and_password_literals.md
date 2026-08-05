---
type: Rust Function
title: login_accepts_username_and_password_literals
resource: crates/lpe-imap/src/tests.rs#L3703-L3730
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/service/ImapServer/with_validator
  - functions/crates/lpe-storage/src/core/Storage/connect
  - functions/crates/lpe-imap/src/tests/read_response
  - functions/crates/lpe-imap/src/tests/send_partial_command
  - functions/crates/lpe-imap/src/tests/send_command
---

# Signature

`async fn login_accepts_username_and_password_literals()`

# Calls

- [with_validator](../../../../../functions/crates/lpe-imap/src/service/ImapServer/with_validator.md)
- [connect](../../../../../functions/crates/lpe-storage/src/core/Storage/connect.md)
- [read_response](../../../../../functions/crates/lpe-imap/src/tests/read_response.md)
- [send_partial_command](../../../../../functions/crates/lpe-imap/src/tests/send_partial_command.md)
- [send_command](../../../../../functions/crates/lpe-imap/src/tests/send_command.md)
---
type: Rust Function
title: authenticate_login_accepts_initial_username_literal
resource: crates/lpe-imap/src/tests.rs#L3733-L3759
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

`async fn authenticate_login_accepts_initial_username_literal()`

# Calls

- [with_validator](../../../../../functions/crates/lpe-imap/src/service/ImapServer/with_validator.md)
- [connect](../../../../../functions/crates/lpe-storage/src/core/Storage/connect.md)
- [read_response](../../../../../functions/crates/lpe-imap/src/tests/read_response.md)
- [send_partial_command](../../../../../functions/crates/lpe-imap/src/tests/send_partial_command.md)
- [send_command](../../../../../functions/crates/lpe-imap/src/tests/send_command.md)
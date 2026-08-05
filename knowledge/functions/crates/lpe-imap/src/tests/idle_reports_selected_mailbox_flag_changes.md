---
type: Rust Function
title: idle_reports_selected_mailbox_flag_changes
resource: crates/lpe-imap/src/tests.rs#L3335-L3369
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
  - functions/crates/lpe-imap/src/tests/FakeStore/next_modseq
  - functions/crates/lpe-imap/src/tests/read_response_with_timeout
---

# Signature

`async fn idle_reports_selected_mailbox_flag_changes()`

# Calls

- [with_validator](../../../../../functions/crates/lpe-imap/src/service/ImapServer/with_validator.md)
- [connect](../../../../../functions/crates/lpe-storage/src/core/Storage/connect.md)
- [read_response](../../../../../functions/crates/lpe-imap/src/tests/read_response.md)
- [send_command](../../../../../functions/crates/lpe-imap/src/tests/send_command.md)
- [send_partial_command](../../../../../functions/crates/lpe-imap/src/tests/send_partial_command.md)
- [next_modseq](../../../../../functions/crates/lpe-imap/src/tests/FakeStore/next_modseq.md)
- [read_response_with_timeout](../../../../../functions/crates/lpe-imap/src/tests/read_response_with_timeout.md)
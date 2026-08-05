---
type: Rust Function
title: thunderbird_delete_draft_by_move_to_trash_removes_drafts_copy
resource: crates/lpe-imap/src/tests.rs#L2144-L2248
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-imap/src/service/ImapServer/with_validator
  - functions/crates/lpe-storage/src/core/Storage/connect
  - functions/crates/lpe-imap/src/tests/read_response
  - functions/crates/lpe-imap/src/tests/send_command
  - functions/crates/lpe-imap/src/tests/assert_documented_capabilities
  - functions/crates/lpe-imap/src/tests/send_partial_command
---

# Signature

`async fn thunderbird_delete_draft_by_move_to_trash_removes_drafts_copy()`

# Calls

- [with_validator](../../../../../functions/crates/lpe-imap/src/service/ImapServer/with_validator.md)
- [connect](../../../../../functions/crates/lpe-storage/src/core/Storage/connect.md)
- [read_response](../../../../../functions/crates/lpe-imap/src/tests/read_response.md)
- [send_command](../../../../../functions/crates/lpe-imap/src/tests/send_command.md)
- [assert_documented_capabilities](../../../../../functions/crates/lpe-imap/src/tests/assert_documented_capabilities.md)
- [send_partial_command](../../../../../functions/crates/lpe-imap/src/tests/send_partial_command.md)
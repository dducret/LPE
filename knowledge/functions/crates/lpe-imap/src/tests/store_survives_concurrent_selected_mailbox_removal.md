---
type: Rust Function
title: store_survives_concurrent_selected_mailbox_removal
resource: crates/lpe-imap/src/tests.rs#L3372-L3435
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-imap/src/tests/FakeStore/enqueue_post_flag_update_action
  - functions/crates/lpe-imap/src/service/ImapServer/with_validator
  - functions/crates/lpe-storage/src/core/Storage/connect
  - functions/crates/lpe-imap/src/tests/read_response
  - functions/crates/lpe-imap/src/tests/send_command
---

# Signature

`async fn store_survives_concurrent_selected_mailbox_removal()`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [enqueue_post_flag_update_action](../../../../../functions/crates/lpe-imap/src/tests/FakeStore/enqueue_post_flag_update_action.md)
- [with_validator](../../../../../functions/crates/lpe-imap/src/service/ImapServer/with_validator.md)
- [connect](../../../../../functions/crates/lpe-storage/src/core/Storage/connect.md)
- [read_response](../../../../../functions/crates/lpe-imap/src/tests/read_response.md)
- [send_command](../../../../../functions/crates/lpe-imap/src/tests/send_command.md)
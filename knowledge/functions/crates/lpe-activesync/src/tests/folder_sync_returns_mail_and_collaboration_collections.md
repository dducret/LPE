---
type: Rust Function
title: folder_sync_returns_mail_and_collaboration_collections
resource: crates/lpe-activesync/src/tests.rs#L3094-L3195
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/FakeStore/sent_mailbox
  - functions/crates/lpe-activesync/src/tests/folder_sync
  - functions/crates/lpe-activesync/src/tests/folder_add
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value
---

# Signature

`async fn folder_sync_returns_mail_and_collaboration_collections()`

# Calls

- [sent_mailbox](../../../../../functions/crates/lpe-activesync/src/tests/FakeStore/sent_mailbox.md)
- [folder_sync](../../../../../functions/crates/lpe-activesync/src/tests/folder_sync.md)
- [folder_add](../../../../../functions/crates/lpe-activesync/src/tests/folder_add.md)
- [text_value](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value.md)
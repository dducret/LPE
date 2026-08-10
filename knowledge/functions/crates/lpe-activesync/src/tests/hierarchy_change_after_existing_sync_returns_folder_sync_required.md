---
type: Rust Function
title: hierarchy_change_after_existing_sync_returns_folder_sync_required
resource: crates/lpe-activesync/src/tests.rs#L4575-L4638
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/folder_sync
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value
  - functions/crates/lpe-activesync/src/tests/collection_sync_key
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-activesync/src/tests/only_sync_collection
---

# Signature

`async fn hierarchy_change_after_existing_sync_returns_folder_sync_required()`

# Calls

- [folder_sync](../../../../../functions/crates/lpe-activesync/src/tests/folder_sync.md)
- [text_value](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value.md)
- [collection_sync_key](../../../../../functions/crates/lpe-activesync/src/tests/collection_sync_key.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [only_sync_collection](../../../../../functions/crates/lpe-activesync/src/tests/only_sync_collection.md)
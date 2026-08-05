---
type: Rust Function
title: push_folder_metadata
resource: crates/lpe-activesync/src/service/folders.rs#L457-L474
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text
  called_by:
  - functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_sync
---

# Signature

`fn push_folder_metadata(node: &mut WbxmlNode, collection: &CollectionDefinition)`

# Calls

- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [with_text](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text.md)

# Called by

- [handle_folder_sync](../../../../../../functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_sync.md)
---
type: Rust Function
title: sync_collection_status_node
resource: crates/lpe-activesync/src/service/sync_helpers.rs#L51-L58
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text
  called_by:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/sync_collection
---

# Signature

`pub(super) fn sync_collection_status_node(collection_id: Option<&str>, status: &str) -> WbxmlNode`

# Calls

- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [with_text](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text.md)

# Called by

- [sync_collection](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/sync_collection.md)
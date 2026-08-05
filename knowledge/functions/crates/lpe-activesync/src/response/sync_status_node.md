---
type: Rust Function
title: sync_status_node
resource: crates/lpe-activesync/src/response.rs#L90-L95
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

`pub(crate) fn sync_status_node(collection_id: &str, status: &str) -> WbxmlNode`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [with_text](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text.md)

# Called by

- [sync_collection](../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/sync_collection.md)
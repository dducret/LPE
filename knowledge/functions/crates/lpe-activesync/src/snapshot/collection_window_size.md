---
type: Rust Function
title: collection_window_size
resource: crates/lpe-activesync/src/snapshot.rs#L583-L591
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value
  called_by:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/sync_collection
---

# Signature

`pub(crate) fn collection_window_size(sync: &WbxmlNode, collection: &WbxmlNode) -> u64`

# Calls

- [text_value](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value.md)

# Called by

- [sync_collection](../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/sync_collection.md)
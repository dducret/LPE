---
type: Rust Function
title: require_sync_collections
resource: crates/lpe-activesync/src/snapshot.rs#L601-L613
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/handle_sync
---

# Signature

`pub(crate) fn require_sync_collections(request: &WbxmlNode) -> Result<Vec<WbxmlNode>>`

# Called by

- [handle_sync](../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/handle_sync.md)
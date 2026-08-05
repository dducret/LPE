---
type: Rust Function
title: completed_sync_state
resource: crates/lpe-activesync/src/service/sync_helpers.rs#L32-L42
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/sync_collection
  - functions/crates/lpe-activesync/src/service/sync_helpers/decode_sync_state
---

# Signature

`pub(super) fn completed_sync_state( collection_state: Vec<CollectionStateEntry>, hierarchy_generation: Option<String>, ) -> StoredSyncState`

# Called by

- [sync_collection](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/sync_collection.md)
- [decode_sync_state](../../../../../../functions/crates/lpe-activesync/src/service/sync_helpers/decode_sync_state.md)
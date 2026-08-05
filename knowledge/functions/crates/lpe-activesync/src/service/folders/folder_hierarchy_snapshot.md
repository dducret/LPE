---
type: Rust Function
title: folder_hierarchy_snapshot
resource: crates/lpe-activesync/src/service/folders.rs#L434-L455
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/snapshot/snapshot_to_value
  called_by:
  - functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_sync
  - functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/store_current_folder_hierarchy
---

# Signature

`fn folder_hierarchy_snapshot(collections: &[CollectionDefinition]) -> Value`

# Calls

- [snapshot_to_value](../../../../../../functions/crates/lpe-activesync/src/snapshot/snapshot_to_value.md)

# Called by

- [handle_folder_sync](../../../../../../functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_sync.md)
- [store_current_folder_hierarchy](../../../../../../functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/store_current_folder_hierarchy.md)
---
type: Rust Method
title: store_current_folder_hierarchy
resource: crates/lpe-activesync/src/service/folders.rs#L413-L431
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/folder_collections
  - functions/crates/lpe-activesync/src/service/folders/folder_hierarchy_snapshot
  called_by:
  - functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_create
  - functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_delete
  - functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_update
---

# Signature

`async fn store_current_folder_hierarchy( &self, account_id: Uuid, device_id: &str, ) -> Result<String>`

# Calls

- [folder_collections](../../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/folder_collections.md)
- [folder_hierarchy_snapshot](../../../../../../../functions/crates/lpe-activesync/src/service/folders/folder_hierarchy_snapshot.md)

# Called by

- [handle_folder_create](../../../../../../../functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_create.md)
- [handle_folder_delete](../../../../../../../functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_delete.md)
- [handle_folder_update](../../../../../../../functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_update.md)
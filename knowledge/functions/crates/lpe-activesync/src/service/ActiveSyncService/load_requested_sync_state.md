---
type: Rust Method
title: load_requested_sync_state
resource: crates/lpe-activesync/src/service.rs#L594-L622
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/sync_collection
  - functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_sync
  - functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_create
  - functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_delete
  - functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_update
  - functions/crates/lpe-activesync/src/service/get_item_estimate/ActiveSyncService/get_item_estimate_response
---

# Signature

`async fn load_requested_sync_state( &self, account_id: Uuid, device_id: &str, collection_id: &str, requested_key: &str, ) -> Result<Option<lpe_storage::ActiveSyncSyncState>>`

# Called by

- [sync_collection](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/sync_collection.md)
- [handle_folder_sync](../../../../../../functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_sync.md)
- [handle_folder_create](../../../../../../functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_create.md)
- [handle_folder_delete](../../../../../../functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_delete.md)
- [handle_folder_update](../../../../../../functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_update.md)
- [get_item_estimate_response](../../../../../../functions/crates/lpe-activesync/src/service/get_item_estimate/ActiveSyncService/get_item_estimate_response.md)
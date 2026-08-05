---
type: Rust Method
title: resolve_collection
resource: crates/lpe-activesync/src/service.rs#L1419-L1429
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/folder_collections
  called_by:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/sync_collection
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/owned_mail_folder
  - functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_create
  - functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_update
  - functions/crates/lpe-activesync/src/service/get_item_estimate/ActiveSyncService/get_item_estimate_response
  - functions/crates/lpe-activesync/src/service/item_operations/ActiveSyncService/handle_item_operations_fetch
  - functions/crates/lpe-activesync/src/service/move_items/ActiveSyncService/handle_move_item
---

# Signature

`async fn resolve_collection( &self, account_id: Uuid, collection_id: &str, ) -> Result<Option<CollectionDefinition>>`

# Calls

- [folder_collections](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/folder_collections.md)

# Called by

- [sync_collection](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/sync_collection.md)
- [owned_mail_folder](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/owned_mail_folder.md)
- [handle_folder_create](../../../../../../functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_create.md)
- [handle_folder_update](../../../../../../functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_update.md)
- [get_item_estimate_response](../../../../../../functions/crates/lpe-activesync/src/service/get_item_estimate/ActiveSyncService/get_item_estimate_response.md)
- [handle_item_operations_fetch](../../../../../../functions/crates/lpe-activesync/src/service/item_operations/ActiveSyncService/handle_item_operations_fetch.md)
- [handle_move_item](../../../../../../functions/crates/lpe-activesync/src/service/move_items/ActiveSyncService/handle_move_item.md)
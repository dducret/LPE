---
type: Rust Method
title: folder_collections
resource: crates/lpe-activesync/src/service.rs#L1364-L1417
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-activesync/src/protocol/ActiveSyncFolderType/from_mailbox_role
  called_by:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/current_hierarchy_generation
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/trash_collection
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/resolve_collection
  - functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_sync
  - functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/store_current_folder_hierarchy
  - functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/handle_ping
---

# Signature

`async fn folder_collections(&self, account_id: Uuid) -> Result<Vec<CollectionDefinition>>`

# Calls

- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [from_mailbox_role](../../../../../../functions/crates/lpe-activesync/src/protocol/ActiveSyncFolderType/from_mailbox_role.md)

# Called by

- [current_hierarchy_generation](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/current_hierarchy_generation.md)
- [trash_collection](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/trash_collection.md)
- [resolve_collection](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/resolve_collection.md)
- [handle_folder_sync](../../../../../../functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_sync.md)
- [store_current_folder_hierarchy](../../../../../../functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/store_current_folder_hierarchy.md)
- [handle_ping](../../../../../../functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/handle_ping.md)
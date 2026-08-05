---
type: Rust Function
title: mail_collection
resource: crates/lpe-activesync/src/snapshot.rs#L615-L617
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/sync_collection
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/collection_state
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/fetch_collection_nodes
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/fetch_collection_states_by_ids
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/owned_mail_folder
  - functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_create
  - functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_update
  - functions/crates/lpe-activesync/src/service/move_items/ActiveSyncService/handle_move_item
  - functions/crates/lpe-activesync/src/service/ping/ping_change_categories
  - functions/crates/lpe-activesync/src/service/sync_helpers/sync_command_supported_for_collection
---

# Signature

`pub(crate) fn mail_collection(collection: &CollectionDefinition) -> bool`

# Called by

- [sync_collection](../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/sync_collection.md)
- [collection_state](../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/collection_state.md)
- [fetch_collection_nodes](../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/fetch_collection_nodes.md)
- [fetch_collection_states_by_ids](../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/fetch_collection_states_by_ids.md)
- [owned_mail_folder](../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/owned_mail_folder.md)
- [handle_folder_create](../../../../../functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_create.md)
- [handle_folder_update](../../../../../functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_update.md)
- [handle_move_item](../../../../../functions/crates/lpe-activesync/src/service/move_items/ActiveSyncService/handle_move_item.md)
- [ping_change_categories](../../../../../functions/crates/lpe-activesync/src/service/ping/ping_change_categories.md)
- [sync_command_supported_for_collection](../../../../../functions/crates/lpe-activesync/src/service/sync_helpers/sync_command_supported_for_collection.md)
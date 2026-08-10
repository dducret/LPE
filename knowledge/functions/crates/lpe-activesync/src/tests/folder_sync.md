---
type: Rust Function
title: folder_sync
resource: crates/lpe-activesync/src/tests.rs#L2066-L2092
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text
  - functions/crates/lpe-activesync/src/tests/decode_response_body
  called_by:
  - functions/crates/lpe-activesync/src/tests/folder_sync_stale_key_returns_status_9
  - functions/crates/lpe-activesync/src/tests/folder_sync_returns_mail_and_collaboration_collections
  - functions/crates/lpe-activesync/src/tests/folder_sync_preserves_nested_mailbox_parent_ids
  - functions/crates/lpe-activesync/src/tests/folder_sync_projects_shared_mailbox_folders_with_hierarchy
  - functions/crates/lpe-activesync/src/tests/stale_folder_sync_key_is_rejected_after_completed_round
  - functions/crates/lpe-activesync/src/tests/folder_create_creates_root_custom_mail_folder_and_advances_hierarchy_key
  - functions/crates/lpe-activesync/src/tests/folder_create_creates_nested_custom_mail_folder
  - functions/crates/lpe-activesync/src/tests/folder_update_renames_custom_mail_folder
  - functions/crates/lpe-activesync/src/tests/folder_update_moves_custom_mail_folder
  - functions/crates/lpe-activesync/src/tests/folder_delete_deletes_custom_mail_folder
  - functions/crates/lpe-activesync/src/tests/folder_mutations_reject_system_mail_folders
  - functions/crates/lpe-activesync/src/tests/folder_mutation_with_stale_hierarchy_key_is_rejected
  - functions/crates/lpe-activesync/src/tests/successful_folder_mutation_advances_device_hierarchy_for_collection_sync
  - functions/crates/lpe-activesync/src/tests/hierarchy_change_after_existing_sync_returns_folder_sync_required
---

# Signature

`async fn folder_sync( service: &ActiveSyncService<FakeStore>, sync_key: &str, device_id: &str, ) -> WbxmlNode`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [with_text](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text.md)
- [decode_response_body](../../../../../functions/crates/lpe-activesync/src/tests/decode_response_body.md)

# Called by

- [folder_sync_stale_key_returns_status_9](../../../../../functions/crates/lpe-activesync/src/tests/folder_sync_stale_key_returns_status_9.md)
- [folder_sync_returns_mail_and_collaboration_collections](../../../../../functions/crates/lpe-activesync/src/tests/folder_sync_returns_mail_and_collaboration_collections.md)
- [folder_sync_preserves_nested_mailbox_parent_ids](../../../../../functions/crates/lpe-activesync/src/tests/folder_sync_preserves_nested_mailbox_parent_ids.md)
- [folder_sync_projects_shared_mailbox_folders_with_hierarchy](../../../../../functions/crates/lpe-activesync/src/tests/folder_sync_projects_shared_mailbox_folders_with_hierarchy.md)
- [stale_folder_sync_key_is_rejected_after_completed_round](../../../../../functions/crates/lpe-activesync/src/tests/stale_folder_sync_key_is_rejected_after_completed_round.md)
- [folder_create_creates_root_custom_mail_folder_and_advances_hierarchy_key](../../../../../functions/crates/lpe-activesync/src/tests/folder_create_creates_root_custom_mail_folder_and_advances_hierarchy_key.md)
- [folder_create_creates_nested_custom_mail_folder](../../../../../functions/crates/lpe-activesync/src/tests/folder_create_creates_nested_custom_mail_folder.md)
- [folder_update_renames_custom_mail_folder](../../../../../functions/crates/lpe-activesync/src/tests/folder_update_renames_custom_mail_folder.md)
- [folder_update_moves_custom_mail_folder](../../../../../functions/crates/lpe-activesync/src/tests/folder_update_moves_custom_mail_folder.md)
- [folder_delete_deletes_custom_mail_folder](../../../../../functions/crates/lpe-activesync/src/tests/folder_delete_deletes_custom_mail_folder.md)
- [folder_mutations_reject_system_mail_folders](../../../../../functions/crates/lpe-activesync/src/tests/folder_mutations_reject_system_mail_folders.md)
- [folder_mutation_with_stale_hierarchy_key_is_rejected](../../../../../functions/crates/lpe-activesync/src/tests/folder_mutation_with_stale_hierarchy_key_is_rejected.md)
- [successful_folder_mutation_advances_device_hierarchy_for_collection_sync](../../../../../functions/crates/lpe-activesync/src/tests/successful_folder_mutation_advances_device_hierarchy_for_collection_sync.md)
- [hierarchy_change_after_existing_sync_returns_folder_sync_required](../../../../../functions/crates/lpe-activesync/src/tests/hierarchy_change_after_existing_sync_returns_folder_sync_required.md)
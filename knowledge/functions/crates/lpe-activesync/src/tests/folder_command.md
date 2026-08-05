---
type: Rust Function
title: folder_command
resource: crates/lpe-activesync/src/tests.rs#L1841-L1857
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/active_sync_query
  - functions/crates/lpe-activesync/src/tests/decode_response_body
  called_by:
  - functions/crates/lpe-activesync/src/tests/folder_create_creates_root_custom_mail_folder_and_advances_hierarchy_key
  - functions/crates/lpe-activesync/src/tests/folder_create_creates_nested_custom_mail_folder
  - functions/crates/lpe-activesync/src/tests/folder_update_renames_custom_mail_folder
  - functions/crates/lpe-activesync/src/tests/folder_update_moves_custom_mail_folder
  - functions/crates/lpe-activesync/src/tests/folder_delete_deletes_custom_mail_folder
  - functions/crates/lpe-activesync/src/tests/folder_mutations_reject_system_mail_folders
  - functions/crates/lpe-activesync/src/tests/folder_mutation_with_stale_hierarchy_key_is_rejected
  - functions/crates/lpe-activesync/src/tests/successful_folder_mutation_advances_device_hierarchy_for_collection_sync
---

# Signature

`async fn folder_command( service: &ActiveSyncService<FakeStore>, command: &str, device_id: &str, request: Vec<u8>, ) -> WbxmlNode`

# Calls

- [active_sync_query](../../../../../functions/crates/lpe-activesync/src/tests/active_sync_query.md)
- [decode_response_body](../../../../../functions/crates/lpe-activesync/src/tests/decode_response_body.md)

# Called by

- [folder_create_creates_root_custom_mail_folder_and_advances_hierarchy_key](../../../../../functions/crates/lpe-activesync/src/tests/folder_create_creates_root_custom_mail_folder_and_advances_hierarchy_key.md)
- [folder_create_creates_nested_custom_mail_folder](../../../../../functions/crates/lpe-activesync/src/tests/folder_create_creates_nested_custom_mail_folder.md)
- [folder_update_renames_custom_mail_folder](../../../../../functions/crates/lpe-activesync/src/tests/folder_update_renames_custom_mail_folder.md)
- [folder_update_moves_custom_mail_folder](../../../../../functions/crates/lpe-activesync/src/tests/folder_update_moves_custom_mail_folder.md)
- [folder_delete_deletes_custom_mail_folder](../../../../../functions/crates/lpe-activesync/src/tests/folder_delete_deletes_custom_mail_folder.md)
- [folder_mutations_reject_system_mail_folders](../../../../../functions/crates/lpe-activesync/src/tests/folder_mutations_reject_system_mail_folders.md)
- [folder_mutation_with_stale_hierarchy_key_is_rejected](../../../../../functions/crates/lpe-activesync/src/tests/folder_mutation_with_stale_hierarchy_key_is_rejected.md)
- [successful_folder_mutation_advances_device_hierarchy_for_collection_sync](../../../../../functions/crates/lpe-activesync/src/tests/successful_folder_mutation_advances_device_hierarchy_for_collection_sync.md)
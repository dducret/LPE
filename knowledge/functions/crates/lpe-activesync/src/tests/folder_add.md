---
type: Rust Function
title: folder_add
resource: crates/lpe-activesync/src/tests.rs#L2127-L2133
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value
  called_by:
  - functions/crates/lpe-activesync/src/tests/folder_sync_returns_mail_and_collaboration_collections
  - functions/crates/lpe-activesync/src/tests/folder_sync_preserves_nested_mailbox_parent_ids
  - functions/crates/lpe-activesync/src/tests/folder_sync_projects_shared_mailbox_folders_with_hierarchy
  - functions/crates/lpe-activesync/src/tests/folder_create_creates_nested_custom_mail_folder
  - functions/crates/lpe-activesync/src/tests/folder_update_renames_custom_mail_folder
  - functions/crates/lpe-activesync/src/tests/folder_update_moves_custom_mail_folder
---

# Signature

`fn folder_add<'a>(changes: &'a WbxmlNode, server_id: &str) -> &'a WbxmlNode`

# Calls

- [text_value](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value.md)

# Called by

- [folder_sync_returns_mail_and_collaboration_collections](../../../../../functions/crates/lpe-activesync/src/tests/folder_sync_returns_mail_and_collaboration_collections.md)
- [folder_sync_preserves_nested_mailbox_parent_ids](../../../../../functions/crates/lpe-activesync/src/tests/folder_sync_preserves_nested_mailbox_parent_ids.md)
- [folder_sync_projects_shared_mailbox_folders_with_hierarchy](../../../../../functions/crates/lpe-activesync/src/tests/folder_sync_projects_shared_mailbox_folders_with_hierarchy.md)
- [folder_create_creates_nested_custom_mail_folder](../../../../../functions/crates/lpe-activesync/src/tests/folder_create_creates_nested_custom_mail_folder.md)
- [folder_update_renames_custom_mail_folder](../../../../../functions/crates/lpe-activesync/src/tests/folder_update_renames_custom_mail_folder.md)
- [folder_update_moves_custom_mail_folder](../../../../../functions/crates/lpe-activesync/src/tests/folder_update_moves_custom_mail_folder.md)
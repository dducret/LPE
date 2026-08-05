---
type: Rust Function
title: folder_create_request
resource: crates/lpe-activesync/src/tests.rs#L1805-L1814
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text
  called_by:
  - functions/crates/lpe-activesync/src/tests/folder_create_creates_root_custom_mail_folder_and_advances_hierarchy_key
  - functions/crates/lpe-activesync/src/tests/folder_create_creates_nested_custom_mail_folder
  - functions/crates/lpe-activesync/src/tests/folder_mutation_with_stale_hierarchy_key_is_rejected
  - functions/crates/lpe-activesync/src/tests/successful_folder_mutation_advances_device_hierarchy_for_collection_sync
---

# Signature

`fn folder_create_request(sync_key: &str, parent_id: &str, display_name: &str) -> Vec<u8>`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [with_text](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text.md)

# Called by

- [folder_create_creates_root_custom_mail_folder_and_advances_hierarchy_key](../../../../../functions/crates/lpe-activesync/src/tests/folder_create_creates_root_custom_mail_folder_and_advances_hierarchy_key.md)
- [folder_create_creates_nested_custom_mail_folder](../../../../../functions/crates/lpe-activesync/src/tests/folder_create_creates_nested_custom_mail_folder.md)
- [folder_mutation_with_stale_hierarchy_key_is_rejected](../../../../../functions/crates/lpe-activesync/src/tests/folder_mutation_with_stale_hierarchy_key_is_rejected.md)
- [successful_folder_mutation_advances_device_hierarchy_for_collection_sync](../../../../../functions/crates/lpe-activesync/src/tests/successful_folder_mutation_advances_device_hierarchy_for_collection_sync.md)
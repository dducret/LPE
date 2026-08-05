---
type: Rust Function
title: folder_update_request
resource: crates/lpe-activesync/src/tests.rs#L1816-L1830
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text
  called_by:
  - functions/crates/lpe-activesync/src/tests/folder_update_renames_custom_mail_folder
  - functions/crates/lpe-activesync/src/tests/folder_update_moves_custom_mail_folder
  - functions/crates/lpe-activesync/src/tests/folder_mutations_reject_system_mail_folders
---

# Signature

`fn folder_update_request( sync_key: &str, server_id: &str, parent_id: &str, display_name: &str, ) -> Vec<u8>`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [with_text](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text.md)

# Called by

- [folder_update_renames_custom_mail_folder](../../../../../functions/crates/lpe-activesync/src/tests/folder_update_renames_custom_mail_folder.md)
- [folder_update_moves_custom_mail_folder](../../../../../functions/crates/lpe-activesync/src/tests/folder_update_moves_custom_mail_folder.md)
- [folder_mutations_reject_system_mail_folders](../../../../../functions/crates/lpe-activesync/src/tests/folder_mutations_reject_system_mail_folders.md)
---
type: Rust Function
title: folder_delete_request
resource: crates/lpe-activesync/src/tests.rs#L1832-L1839
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text
  called_by:
  - functions/crates/lpe-activesync/src/tests/folder_delete_deletes_custom_mail_folder
  - functions/crates/lpe-activesync/src/tests/folder_mutations_reject_system_mail_folders
---

# Signature

`fn folder_delete_request(sync_key: &str, server_id: &str) -> Vec<u8>`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [with_text](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text.md)

# Called by

- [folder_delete_deletes_custom_mail_folder](../../../../../functions/crates/lpe-activesync/src/tests/folder_delete_deletes_custom_mail_folder.md)
- [folder_mutations_reject_system_mail_folders](../../../../../functions/crates/lpe-activesync/src/tests/folder_mutations_reject_system_mail_folders.md)
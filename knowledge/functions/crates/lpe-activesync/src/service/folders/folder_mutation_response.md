---
type: Rust Function
title: folder_mutation_response
resource: crates/lpe-activesync/src/service/folders.rs#L476-L494
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text
  - functions/crates/lpe-activesync/src/response/wbxml_response
  called_by:
  - functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_create
  - functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_delete
  - functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_update
---

# Signature

`fn folder_mutation_response( protocol_version: &str, command: &str, status: &str, sync_key: Option<&str>, server_id: Option<&str>, ) -> Result<Response>`

# Calls

- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [with_text](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text.md)
- [wbxml_response](../../../../../../functions/crates/lpe-activesync/src/response/wbxml_response.md)

# Called by

- [handle_folder_create](../../../../../../functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_create.md)
- [handle_folder_delete](../../../../../../functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_delete.md)
- [handle_folder_update](../../../../../../functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_update.md)
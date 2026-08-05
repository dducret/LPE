---
type: Rust Function
title: command_status_response
resource: crates/lpe-activesync/src/service.rs#L1445-L1454
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text
  - functions/crates/lpe-activesync/src/response/wbxml_response
  called_by:
  - functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_sync
  - functions/crates/lpe-activesync/src/service/item_operations/ActiveSyncService/handle_item_operations
  - functions/crates/lpe-activesync/src/service/move_items/ActiveSyncService/handle_move_items
  - functions/crates/lpe-activesync/src/service/provisioning/ActiveSyncService/handle_provision
  - functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/handle_send_mail
  - functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/handle_smart_compose
---

# Signature

`fn command_status_response( protocol_version: &str, page: u8, command: &str, status: &str, ) -> Result<Response>`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [with_text](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text.md)
- [wbxml_response](../../../../../functions/crates/lpe-activesync/src/response/wbxml_response.md)

# Called by

- [handle_folder_sync](../../../../../functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_sync.md)
- [handle_item_operations](../../../../../functions/crates/lpe-activesync/src/service/item_operations/ActiveSyncService/handle_item_operations.md)
- [handle_move_items](../../../../../functions/crates/lpe-activesync/src/service/move_items/ActiveSyncService/handle_move_items.md)
- [handle_provision](../../../../../functions/crates/lpe-activesync/src/service/provisioning/ActiveSyncService/handle_provision.md)
- [handle_send_mail](../../../../../functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/handle_send_mail.md)
- [handle_smart_compose](../../../../../functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/handle_smart_compose.md)
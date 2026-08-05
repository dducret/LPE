---
type: Rust Method
title: handle_folder_delete
resource: crates/lpe-activesync/src/service/folders.rs#L232-L301
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value
  - functions/crates/lpe-activesync/src/service/folders/folder_mutation_response
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/load_requested_sync_state
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/owned_mail_folder
  - functions/crates/lpe-activesync/src/service/folders/active_sync_audit
  - functions/crates/lpe-activesync/src/service/folders/folder_delete_error_status
  - functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/store_current_folder_hierarchy
  called_by:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/handle_parsed_request
---

# Signature

`pub(super) async fn handle_folder_delete( &self, principal: &AuthenticatedPrincipal, device_id: &str, protocol_version: &str, request: &WbxmlNode, ) -> Result<Response>`

# Calls

- [text_value](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value.md)
- [folder_mutation_response](../../../../../../../functions/crates/lpe-activesync/src/service/folders/folder_mutation_response.md)
- [load_requested_sync_state](../../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/load_requested_sync_state.md)
- [owned_mail_folder](../../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/owned_mail_folder.md)
- [active_sync_audit](../../../../../../../functions/crates/lpe-activesync/src/service/folders/active_sync_audit.md)
- [folder_delete_error_status](../../../../../../../functions/crates/lpe-activesync/src/service/folders/folder_delete_error_status.md)
- [store_current_folder_hierarchy](../../../../../../../functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/store_current_folder_hierarchy.md)

# Called by

- [handle_parsed_request](../../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/handle_parsed_request.md)
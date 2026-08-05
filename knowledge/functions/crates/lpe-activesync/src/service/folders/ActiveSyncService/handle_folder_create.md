---
type: Rust Method
title: handle_folder_create
resource: crates/lpe-activesync/src/service/folders.rs#L120-L230
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value
  - functions/crates/lpe-activesync/src/service/folders/folder_mutation_response
  - functions/crates/lpe-domain/src/mailbox_name/MailboxNamePolicy/system_role_for_display_name
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/load_requested_sync_state
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/resolve_collection
  - functions/crates/lpe-activesync/src/snapshot/mail_collection
  - functions/crates/lpe-activesync/src/service/folders/active_sync_audit
  - functions/crates/lpe-activesync/src/service/folders/folder_create_error_status
  - functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/store_current_folder_hierarchy
  called_by:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/handle_parsed_request
---

# Signature

`pub(super) async fn handle_folder_create( &self, principal: &AuthenticatedPrincipal, device_id: &str, protocol_version: &str, request: &WbxmlNode, ) -> Result<Response>`

# Calls

- [text_value](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value.md)
- [folder_mutation_response](../../../../../../../functions/crates/lpe-activesync/src/service/folders/folder_mutation_response.md)
- [system_role_for_display_name](../../../../../../../functions/crates/lpe-domain/src/mailbox_name/MailboxNamePolicy/system_role_for_display_name.md)
- [load_requested_sync_state](../../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/load_requested_sync_state.md)
- [resolve_collection](../../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/resolve_collection.md)
- [mail_collection](../../../../../../../functions/crates/lpe-activesync/src/snapshot/mail_collection.md)
- [active_sync_audit](../../../../../../../functions/crates/lpe-activesync/src/service/folders/active_sync_audit.md)
- [folder_create_error_status](../../../../../../../functions/crates/lpe-activesync/src/service/folders/folder_create_error_status.md)
- [store_current_folder_hierarchy](../../../../../../../functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/store_current_folder_hierarchy.md)

# Called by

- [handle_parsed_request](../../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/handle_parsed_request.md)
---
type: Rust Method
title: handle_parsed_request
resource: crates/lpe-activesync/src/service.rs#L163-L322
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/auth/protocol_version
  - functions/crates/lpe-activesync/src/auth/ensure_supported_protocol_version
  - functions/crates/lpe-activesync/src/service/provisioning/header_policy_key
  - functions/crates/lpe-activesync/src/service/provisioning/ActiveSyncService/policy_key_is_current
  - functions/crates/lpe-activesync/src/service/provisioning/policy_required_response
  - functions/crates/lpe-activesync/src/wbxml/decode_wbxml
  - functions/crates/lpe-activesync/src/service/provisioning/ActiveSyncService/handle_provision
  - functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_sync
  - functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_create
  - functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_delete
  - functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_update
  - functions/crates/lpe-activesync/src/service/get_item_estimate/ActiveSyncService/handle_get_item_estimate
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/handle_sync
  - functions/crates/lpe-activesync/src/service/item_operations/ActiveSyncService/handle_item_operations
  - functions/crates/lpe-activesync/src/service/move_items/ActiveSyncService/handle_move_items
  - functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/handle_ping
  - functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/handle_send_mail
  - functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/handle_smart_compose
  - functions/crates/lpe-activesync/src/protocol/ActiveSyncCommand/known_unsupported_name_for_str
  called_by:
  - functions/crates/lpe-activesync/src/app/post_handler
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/handle_request
  - functions/crates/lpe-activesync/src/tests/handle_base64_request
---

# Signature

`pub(crate) async fn handle_parsed_request( &self, parsed: ParsedActiveSyncQuery, headers: &HeaderMap, body: &[u8], ) -> Result<Response>`

# Calls

- [protocol_version](../../../../../../functions/crates/lpe-activesync/src/auth/protocol_version.md)
- [ensure_supported_protocol_version](../../../../../../functions/crates/lpe-activesync/src/auth/ensure_supported_protocol_version.md)
- [header_policy_key](../../../../../../functions/crates/lpe-activesync/src/service/provisioning/header_policy_key.md)
- [policy_key_is_current](../../../../../../functions/crates/lpe-activesync/src/service/provisioning/ActiveSyncService/policy_key_is_current.md)
- [policy_required_response](../../../../../../functions/crates/lpe-activesync/src/service/provisioning/policy_required_response.md)
- [decode_wbxml](../../../../../../functions/crates/lpe-activesync/src/wbxml/decode_wbxml.md)
- [handle_provision](../../../../../../functions/crates/lpe-activesync/src/service/provisioning/ActiveSyncService/handle_provision.md)
- [handle_folder_sync](../../../../../../functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_sync.md)
- [handle_folder_create](../../../../../../functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_create.md)
- [handle_folder_delete](../../../../../../functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_delete.md)
- [handle_folder_update](../../../../../../functions/crates/lpe-activesync/src/service/folders/ActiveSyncService/handle_folder_update.md)
- [handle_get_item_estimate](../../../../../../functions/crates/lpe-activesync/src/service/get_item_estimate/ActiveSyncService/handle_get_item_estimate.md)
- [handle_sync](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/handle_sync.md)
- [handle_item_operations](../../../../../../functions/crates/lpe-activesync/src/service/item_operations/ActiveSyncService/handle_item_operations.md)
- [handle_move_items](../../../../../../functions/crates/lpe-activesync/src/service/move_items/ActiveSyncService/handle_move_items.md)
- [handle_ping](../../../../../../functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/handle_ping.md)
- [handle_send_mail](../../../../../../functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/handle_send_mail.md)
- [handle_smart_compose](../../../../../../functions/crates/lpe-activesync/src/service/submission/ActiveSyncService/handle_smart_compose.md)
- [known_unsupported_name_for_str](../../../../../../functions/crates/lpe-activesync/src/protocol/ActiveSyncCommand/known_unsupported_name_for_str.md)

# Called by

- [post_handler](../../../../../../functions/crates/lpe-activesync/src/app/post_handler.md)
- [handle_request](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/handle_request.md)
- [handle_base64_request](../../../../../../functions/crates/lpe-activesync/src/tests/handle_base64_request.md)
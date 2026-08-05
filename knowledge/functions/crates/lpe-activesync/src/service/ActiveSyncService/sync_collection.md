---
type: Rust Method
title: sync_collection
resource: crates/lpe-activesync/src/service.rs#L361-L573
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/service/sync_helpers/sync_collection_status_node
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value
  - functions/crates/lpe-activesync/src/snapshot/collection_window_size
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/current_hierarchy_generation
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/resolve_collection
  - functions/crates/lpe-activesync/src/response/sync_status_node
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/load_requested_sync_state
  - functions/crates/lpe-activesync/src/service/sync_helpers/decode_sync_state
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/device_hierarchy_is_current
  - functions/crates/lpe-activesync/src/service/body_preferences/collection_body_preference
  - functions/crates/lpe-activesync/src/service/sync_helpers/sync_collection_has_unsupported_command
  - functions/crates/lpe-activesync/src/snapshot/drafts_collection
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/apply_draft_sync_commands
  - functions/crates/lpe-activesync/src/snapshot/mail_collection
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/apply_mail_sync_commands
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/apply_contact_sync_commands
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/apply_calendar_sync_commands
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/collection_state
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text
  - functions/crates/lpe-activesync/src/service/sync_helpers/has_client_commands
  - functions/crates/lpe-activesync/src/snapshot/diff_collection_states
  - functions/crates/lpe-activesync/src/service/sync_helpers/completed_sync_state
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/store_sync_state
  - functions/crates/lpe-activesync/src/service/sync_helpers/pending_page
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/pending_page_is_stable
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/build_commands
---

# Signature

`async fn sync_collection( &self, principal: &AuthenticatedPrincipal, device_id: &str, request: &WbxmlNode, collection_node: &WbxmlNode, ) -> Result<WbxmlNode>`

# Calls

- [sync_collection_status_node](../../../../../../functions/crates/lpe-activesync/src/service/sync_helpers/sync_collection_status_node.md)
- [text_value](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value.md)
- [collection_window_size](../../../../../../functions/crates/lpe-activesync/src/snapshot/collection_window_size.md)
- [current_hierarchy_generation](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/current_hierarchy_generation.md)
- [resolve_collection](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/resolve_collection.md)
- [sync_status_node](../../../../../../functions/crates/lpe-activesync/src/response/sync_status_node.md)
- [load_requested_sync_state](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/load_requested_sync_state.md)
- [decode_sync_state](../../../../../../functions/crates/lpe-activesync/src/service/sync_helpers/decode_sync_state.md)
- [device_hierarchy_is_current](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/device_hierarchy_is_current.md)
- [collection_body_preference](../../../../../../functions/crates/lpe-activesync/src/service/body_preferences/collection_body_preference.md)
- [sync_collection_has_unsupported_command](../../../../../../functions/crates/lpe-activesync/src/service/sync_helpers/sync_collection_has_unsupported_command.md)
- [drafts_collection](../../../../../../functions/crates/lpe-activesync/src/snapshot/drafts_collection.md)
- [apply_draft_sync_commands](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/apply_draft_sync_commands.md)
- [mail_collection](../../../../../../functions/crates/lpe-activesync/src/snapshot/mail_collection.md)
- [apply_mail_sync_commands](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/apply_mail_sync_commands.md)
- [apply_contact_sync_commands](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/apply_contact_sync_commands.md)
- [apply_calendar_sync_commands](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/apply_calendar_sync_commands.md)
- [collection_state](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/collection_state.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [with_text](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text.md)
- [has_client_commands](../../../../../../functions/crates/lpe-activesync/src/service/sync_helpers/has_client_commands.md)
- [diff_collection_states](../../../../../../functions/crates/lpe-activesync/src/snapshot/diff_collection_states.md)
- [completed_sync_state](../../../../../../functions/crates/lpe-activesync/src/service/sync_helpers/completed_sync_state.md)
- [store_sync_state](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/store_sync_state.md)
- [pending_page](../../../../../../functions/crates/lpe-activesync/src/service/sync_helpers/pending_page.md)
- [pending_page_is_stable](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/pending_page_is_stable.md)
- [build_commands](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/build_commands.md)
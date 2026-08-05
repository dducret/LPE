---
type: Rust Function
title: append_pending_associated_config_save_response
resource: crates/lpe-exchange/src/mapi/dispatch/associated_config.rs#L327-L431
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/persist_associated_config_message
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_post_hierarchy_create_save_object_context
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/associated_config_message_with_identity
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/record_sync_upload_content_change
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/record_notification
  - functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/content
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_save_changes_message_response
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/append_pending_navigation_shortcut_save_response
---

# Signature

`pub(super) async fn append_pending_associated_config_save_response<S: ExchangeStore>( store: &S, principal: &AccountPrincipal, mapi_request_id: &str, session: &mut MapiSession, handle_slots: &mut Vec<u32>, request: &RopRequest, responses: &mut Vec<u8>, handle: u32, folder_id: u64, properties: &HashMap<u32, MapiValue>, imported_message_id: Option<u64>, fail_on_conflict: bool, )`

# Calls

- [persist_associated_config_message](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/persist_associated_config_message.md)
- [record_last_post_hierarchy_create_save_object_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_post_hierarchy_create_save_object_context.md)
- [associated_config_message_with_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/associated_config_message_with_identity.md)
- [record_sync_upload_content_change](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/record_sync_upload_content_change.md)
- [record_notification](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/record_notification.md)
- [content](../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/content.md)
- [append_save_changes_message_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_save_changes_message_response.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)

# Called by

- [append_save_changes_message_route_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response.md)
- [append_pending_navigation_shortcut_save_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/append_pending_navigation_shortcut_save_response.md)
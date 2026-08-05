---
type: Rust Function
title: append_existing_navigation_shortcut_save_response
resource: crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save.rs#L508-L631
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_save_changes_message_response
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/navigation_shortcut_message_for_id
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/properties/navigation_shortcut/navigation_shortcut_properties_with_pending
  - functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/validated_navigation_shortcut_from_mapi_properties
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_navigation_shortcut_update
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity_with_source_key
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/remember_navigation_shortcut
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/record_notification
  - functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/content
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response
---

# Signature

`pub(super) async fn append_existing_navigation_shortcut_save_response<S: ExchangeStore>( store: &S, principal: &AccountPrincipal, session: &mut MapiSession, handle_slots: &mut Vec<u32>, request: &RopRequest, snapshot: &mut MapiMailStoreSnapshot, responses: &mut Vec<u8>, handle: u32, folder_id: u64, shortcut_id: u64, pending_properties: HashMap<u32, MapiValue>, deleted_properties: HashSet<u32>, )`

# Calls

- [append_save_changes_message_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_save_changes_message_response.md)
- [navigation_shortcut_message_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/navigation_shortcut_message_for_id.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [navigation_shortcut_properties_with_pending](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/navigation_shortcut/navigation_shortcut_properties_with_pending.md)
- [validated_navigation_shortcut_from_mapi_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/validated_navigation_shortcut_from_mapi_properties.md)
- [commit_mapi_navigation_shortcut_update](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_navigation_shortcut_update.md)
- [remember_mapi_identity_with_source_key](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity_with_source_key.md)
- [remember_navigation_shortcut](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/remember_navigation_shortcut.md)
- [record_notification](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/record_notification.md)
- [content](../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/content.md)

# Called by

- [append_save_changes_message_route_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response.md)
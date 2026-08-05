---
type: Rust Function
title: append_pending_navigation_shortcut_save_response
resource: crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save.rs#L262-L505
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/pending_common_views_message_is_navigation_shortcut
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/append_pending_associated_config_save_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/validated_navigation_shortcut_from_mapi_properties
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/navigation_shortcut_message_for_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/imported_fai_identity
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_navigation_shortcut_import
  - functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity_with_source_key
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_navigation_shortcut_create
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_post_hierarchy_create_save_object_context
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/remember_navigation_shortcut
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/record_sync_upload_content_change
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/record_notification
  - functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/content
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_save_changes_message_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response
---

# Signature

`pub(super) async fn append_pending_navigation_shortcut_save_response<S: ExchangeStore>( store: &S, principal: &AccountPrincipal, mapi_request_id: &str, session: &mut MapiSession, handle_slots: &mut Vec<u32>, request: &RopRequest, snapshot: &mut MapiMailStoreSnapshot, responses: &mut Vec<u8>, handle: u32, folder_id: u64, properties: HashMap<u32, MapiValue>, imported_message_id: Option<u64>, fail_on_conflict: bool, )`

# Calls

- [pending_common_views_message_is_navigation_shortcut](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/pending_common_views_message_is_navigation_shortcut.md)
- [append_pending_associated_config_save_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/append_pending_associated_config_save_response.md)
- [validated_navigation_shortcut_from_mapi_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/validated_navigation_shortcut_from_mapi_properties.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [navigation_shortcut_message_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/navigation_shortcut_message_for_id.md)
- [imported_fai_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/imported_fai_identity.md)
- [commit_mapi_navigation_shortcut_import](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_navigation_shortcut_import.md)
- [remember_mapi_identity_with_source_key](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/remember_mapi_identity_with_source_key.md)
- [commit_mapi_navigation_shortcut_create](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_navigation_shortcut_create.md)
- [record_last_post_hierarchy_create_save_object_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_last_post_hierarchy_create_save_object_context.md)
- [remember_navigation_shortcut](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/remember_navigation_shortcut.md)
- [record_sync_upload_content_change](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/record_sync_upload_content_change.md)
- [record_notification](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/record_notification.md)
- [content](../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/content.md)
- [append_save_changes_message_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_save_changes_message_response.md)

# Called by

- [append_save_changes_message_route_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response.md)
---
type: Rust Function
title: save_pending_contact
resource: crates/lpe-exchange/src/mapi/dispatch/contact_save.rs#L6-L127
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/properties/contact/contact_input_from_mapi
  - functions/crates/lpe-exchange/src/mapi/properties/contact/default_contact_for_mapping
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/mapi_contact_custom_property_values_from_map
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/remember_created_contact
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/record_notification
  - functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/content
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/record_sync_upload_content_change
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_save_changes_message_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response
---

# Signature

`pub(super) async fn save_pending_contact<S: ExchangeStore>( store: &S, principal: &AccountPrincipal, session: &mut MapiSession, handle_slots: &mut Vec<u32>, request: &RopRequest, snapshot: &mut MapiMailStoreSnapshot, responses: &mut Vec<u8>, handle: u32, folder_id: u64, properties: HashMap<u32, MapiValue>, imported_identity: Option<lpe_storage::MapiContactImportedIdentity>, fail_on_conflict: bool, )`

# Calls

- [collaboration_folder_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [contact_input_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/contact/contact_input_from_mapi.md)
- [default_contact_for_mapping](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/contact/default_contact_for_mapping.md)
- [mapi_contact_custom_property_values_from_map](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/mapi_contact_custom_property_values_from_map.md)
- [remember_created_contact](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/remember_created_contact.md)
- [record_notification](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/record_notification.md)
- [content](../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/content.md)
- [record_sync_upload_content_change](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/record_sync_upload_content_change.md)
- [append_save_changes_message_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_save_changes_message_response.md)

# Called by

- [append_save_changes_message_route_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response.md)
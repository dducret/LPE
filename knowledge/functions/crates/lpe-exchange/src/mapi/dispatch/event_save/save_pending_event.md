---
type: Rust Function
title: save_pending_event
resource: crates/lpe-exchange/src/mapi/dispatch/event_save.rs#L3-L212
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/imported_event_identity_from_properties
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id
  - functions/crates/lpe-exchange/src/mapi/properties/reminders/split_reminder_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_save/imported_event_global_object_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/event_for_uid
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/imported_event_last_modification_filetime
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/imported_event_transaction
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_save/imported_event_content_properties
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_existing_event
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/event_input_from_mapi
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/default_event_for_mapping
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/apply_calendar_pending_recipients
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/mapi_event_custom_property_values_from_map
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/remember_created_event
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/remember_event_version
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/remember_event_reminder_state
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/save_disposition
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_save/remember_saved_event_handle
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/clear_event_attachment_transaction
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/record_notification
  - functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/content
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/record_sync_upload_content_change
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_save_changes_message_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response
---

# Signature

`pub(super) async fn save_pending_event<S: ExchangeStore>( store: &S, principal: &AccountPrincipal, session: &mut MapiSession, handle_slots: &mut Vec<u32>, request: &RopRequest, snapshot: &mut MapiMailStoreSnapshot, responses: &mut Vec<u8>, handle: u32, folder_id: u64, properties: HashMap<u32, MapiValue>, recipients: Vec<PendingRecipient>, recipients_modified: bool, fail_on_conflict: bool, )`

# Calls

- [imported_event_identity_from_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/imported_event_identity_from_properties.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [collaboration_folder_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id.md)
- [split_reminder_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/reminders/split_reminder_property_values.md)
- [imported_event_global_object_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_save/imported_event_global_object_id.md)
- [event_for_uid](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/event_for_uid.md)
- [imported_event_last_modification_filetime](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/imported_event_last_modification_filetime.md)
- [imported_event_transaction](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/imported_event_transaction.md)
- [imported_event_content_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_save/imported_event_content_properties.md)
- [save_existing_event](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_existing_event.md)
- [event_input_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/event_input_from_mapi.md)
- [default_event_for_mapping](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/default_event_for_mapping.md)
- [apply_calendar_pending_recipients](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/apply_calendar_pending_recipients.md)
- [mapi_event_custom_property_values_from_map](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/mapi_event_custom_property_values_from_map.md)
- [remember_created_event](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/remember_created_event.md)
- [remember_event_version](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/remember_event_version.md)
- [remember_event_reminder_state](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/remember_event_reminder_state.md)
- [save_disposition](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/save_disposition.md)
- [expect](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [remember_saved_event_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_save/remember_saved_event_handle.md)
- [clear_event_attachment_transaction](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/clear_event_attachment_transaction.md)
- [record_notification](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/record_notification.md)
- [content](../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/content.md)
- [record_sync_upload_content_change](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/record_sync_upload_content_change.md)
- [append_save_changes_message_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_save_changes_message_response.md)

# Called by

- [append_save_changes_message_route_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response.md)
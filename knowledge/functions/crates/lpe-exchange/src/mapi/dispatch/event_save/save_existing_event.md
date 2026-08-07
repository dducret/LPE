---
type: Rust Function
title: save_existing_event
resource: crates/lpe-exchange/src/mapi/dispatch/event_save.rs#L146-L332
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/event_for_id
  - functions/crates/lpe-exchange/src/mapi/event_metrics/record_mapi_calendar_event_save
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/event_handle_is_writable
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/save_disposition
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/staged_event_commit_input
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/reminder_for_source
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/clear_event_attachment_transaction
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_save/remember_saved_event_handle
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_save_changes_message_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/event_after_commit
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/remember_updated_event
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/remember_event_reminder_state
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/record_notification
  - functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/content
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/record_sync_upload_content_change
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response
---

# Signature

`pub(super) async fn save_existing_event<S: ExchangeStore>( store: &S, principal: &AccountPrincipal, session: &mut MapiSession, handle_slots: &mut Vec<u32>, request: &RopRequest, snapshot: &mut MapiMailStoreSnapshot, responses: &mut Vec<u8>, handle: u32, folder_id: u64, event_id: u64, transaction: MapiEventTransaction, )`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [event_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/event_for_id.md)
- [record_mapi_calendar_event_save](../../../../../../../functions/crates/lpe-exchange/src/mapi/event_metrics/record_mapi_calendar_event_save.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [event_handle_is_writable](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/event_handle_is_writable.md)
- [save_disposition](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/save_disposition.md)
- [expect](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [staged_event_commit_input](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/staged_event_commit_input.md)
- [reminder_for_source](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/reminder_for_source.md)
- [clear_event_attachment_transaction](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/clear_event_attachment_transaction.md)
- [remember_saved_event_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_save/remember_saved_event_handle.md)
- [append_save_changes_message_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_save_changes_message_response.md)
- [event_after_commit](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/event_after_commit.md)
- [remember_updated_event](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/remember_updated_event.md)
- [remember_event_reminder_state](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/remember_event_reminder_state.md)
- [record_notification](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/record_notification.md)
- [content](../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/content.md)
- [record_sync_upload_content_change](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/record_sync_upload_content_change.md)

# Called by

- [append_save_changes_message_route_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response.md)
---
type: Rust Function
title: append_existing_associated_config_save_response
resource: crates/lpe-exchange/src/mapi/dispatch/associated_config.rs#L434-L555
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/associated_config_message_for_mutation
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/associated_config_mutation_base_properties
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/associated_config_class_and_subject
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/normalized_associated_config_content_properties
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_save_changes_message_response
  - functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_associated_config_update
  - functions/crates/lpe-exchange/src/mapi/properties/values/mapi_properties_to_json
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/associated_config_message_with_identity
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/record_sync_upload_content_change
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/record_notification
  - functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/content
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response
---

# Signature

`pub(super) async fn append_existing_associated_config_save_response<S: ExchangeStore>( store: &S, principal: &AccountPrincipal, session: &mut MapiSession, handle_slots: &mut Vec<u32>, request: &RopRequest, snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, handle: u32, folder_id: u64, config_id: u64, saved_message: Option<&crate::mapi_store::MapiAssociatedConfigMessage>, )`

# Calls

- [associated_config_message_for_mutation](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/associated_config_message_for_mutation.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [associated_config_mutation_base_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/associated_config_mutation_base_properties.md)
- [associated_config_class_and_subject](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/associated_config_class_and_subject.md)
- [normalized_associated_config_content_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/normalized_associated_config_content_properties.md)
- [append_save_changes_message_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_save_changes_message_response.md)
- [commit_mapi_associated_config_update](../../../../../../../functions/crates/lpe-exchange/src/tests/FakeStore/exchangestore/commit_mapi_associated_config_update.md)
- [mapi_properties_to_json](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/mapi_properties_to_json.md)
- [associated_config_message_with_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/associated_config_message_with_identity.md)
- [record_sync_upload_content_change](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/record_sync_upload_content_change.md)
- [record_notification](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/record_notification.md)
- [content](../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/MapiNotificationEvent/content.md)

# Called by

- [append_save_changes_message_route_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response.md)
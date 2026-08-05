---
type: Rust Method
title: event_for_id
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L988-L998
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/reminder_events
  - functions/crates/lpe-exchange/src/mapi_store/mapi_event_id_matches
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_get_valid_attachments_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_get_attachment_table_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_open_attachment_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_create_attachment_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_delete_attachment_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/calendar_move_copy/calendar_same_folder_move_partial_completion
  - functions/crates/lpe-exchange/src/mapi/dispatch/calendar_move_copy/calendar_move_to_deleted_items_partial_completion
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/custom_property_storage_account_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/custom_property_object_identity
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_existing_event
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/stage_event_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/stage_event_property_deletions
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_delete_messages_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_message_status_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/object_ids/debug_object_scope_for_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/persisted_object_property_delete_is_idempotent
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/append_synchronization_import_message_change_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message_move/append_synchronization_import_message_move_response
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/apply_canonical_event_property_values
  - functions/crates/lpe-exchange/src/mapi/properties/streams/property_stream_data
  - functions/crates/lpe-exchange/src/mapi/properties/streams/message_body_stream_data
  - functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom
  - functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_all_response
  - functions/crates/lpe-exchange/src/mapi/rop/attachments/rop_get_valid_attachments_response
  - functions/crates/lpe-exchange/src/mapi/rop/event_properties/serialize_event_object_property
  - functions/crates/lpe-exchange/src/mapi_store/tests/exact_event_mid_wins_over_another_events_foreign_cached_alias
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_move_to_deleted_items_rekeys_and_projects_canonical_event
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar_identity_scope/mapi_calendar_snapshot_identity_is_principal_scoped_in_postgresql
---

# Signature

`pub(crate) fn event_for_id(&self, folder_id: u64, item_id: u64) -> Option<&MapiEvent>`

# Calls

- [reminder_events](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/reminder_events.md)
- [mapi_event_id_matches](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/mapi_event_id_matches.md)

# Called by

- [append_get_valid_attachments_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_get_valid_attachments_response.md)
- [append_get_attachment_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_get_attachment_table_response.md)
- [append_open_attachment_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_open_attachment_response.md)
- [append_create_attachment_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_create_attachment_response.md)
- [append_delete_attachment_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_delete_attachment_response.md)
- [calendar_same_folder_move_partial_completion](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/calendar_move_copy/calendar_same_folder_move_partial_completion.md)
- [calendar_move_to_deleted_items_partial_completion](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/calendar_move_copy/calendar_move_to_deleted_items_partial_completion.md)
- [custom_property_storage_account_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/custom_property_storage_account_id.md)
- [custom_property_object_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/custom_properties/custom_property_object_identity.md)
- [save_existing_event](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_existing_event.md)
- [stage_event_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/stage_event_property_values.md)
- [stage_event_property_deletions](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/stage_event_property_deletions.md)
- [append_open_message_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_open/append_open_message_response.md)
- [append_delete_messages_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_delete_messages_response.md)
- [append_message_status_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_message_status_response.md)
- [debug_object_scope_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/object_ids/debug_object_scope_for_id.md)
- [persisted_object_property_delete_is_idempotent](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/persisted_object_property_delete_is_idempotent.md)
- [append_synchronization_import_message_change_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/append_synchronization_import_message_change_response.md)
- [append_synchronization_import_message_move_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message_move/append_synchronization_import_message_move_response.md)
- [apply_canonical_event_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/apply_canonical_event_property_values.md)
- [property_stream_data](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/property_stream_data.md)
- [message_body_stream_data](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/message_body_stream_data.md)
- [rop_get_properties_specific_response_with_custom](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom.md)
- [rop_get_properties_all_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_all_response.md)
- [rop_get_valid_attachments_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/attachments/rop_get_valid_attachments_response.md)
- [serialize_event_object_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/event_properties/serialize_event_object_property.md)
- [exact_event_mid_wins_over_another_events_foreign_cached_alias](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/exact_event_mid_wins_over_another_events_foreign_cached_alias.md)
- [mapi_over_http_calendar_move_to_deleted_items_rekeys_and_projects_canonical_event](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_move_to_deleted_items_rekeys_and_projects_canonical_event.md)
- [mapi_calendar_snapshot_identity_is_principal_scoped_in_postgresql](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar_identity_scope/mapi_calendar_snapshot_identity_is_principal_scoped_in_postgresql.md)
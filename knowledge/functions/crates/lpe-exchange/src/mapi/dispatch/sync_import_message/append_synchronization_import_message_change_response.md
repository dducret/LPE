---
type: Rust Function
title: append_synchronization_import_message_change_response
resource: crates/lpe-exchange/src/mapi/dispatch/sync_import_message.rs#L130-L631
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_object
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_property_values
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_flag
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/imported_property_source_key_global_counter
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_message_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/imported_fai_identity
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/current_common_views_fai_identity
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_conflicts/sync_import_version_relation
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/allocate_output_handle
  - functions/crates/lpe-exchange/src/mapi/session/set_handle_slot
  - functions/crates/lpe-exchange/src/mapi/sync/responses/rop_synchronization_import_message_change_response
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/properties/apply_pending_associated_message_property_values
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/event_for_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/imported_event_identity_from_properties
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/imported_event_last_modification_filetime
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/imported_event_transaction
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/import_message_change_conflicts_with_current_pcl
  - functions/crates/lpe-exchange/src/mapi/properties/message/apply_canonical_message_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/record_sync_upload_content_change
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_item_for_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/apply_canonical_public_folder_item_property_values
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/note_for_id
  - functions/crates/lpe-exchange/src/mapi/properties/notes/apply_canonical_note_property_values
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/journal_entry_for_id
  - functions/crates/lpe-exchange/src/mapi/properties/notes/apply_canonical_journal_entry_property_values
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/imported_contact_identity
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_sync_import_dispatch_response
---

# Signature

`pub(super) async fn append_synchronization_import_message_change_response<S: ExchangeStore>( store: &S, principal: &AccountPrincipal, session: &mut MapiSession, handle_slots: &mut Vec<u32>, request: &RopRequest, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, responses: &mut Vec<u8>, output_handles: &mut Vec<u32>, )`

# Calls

- [input_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)
- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [import_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_property_values.md)
- [import_flag](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_flag.md)
- [imported_property_source_key_global_counter](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/imported_property_source_key_global_counter.md)
- [import_message_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_message_id.md)
- [imported_fai_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/imported_fai_identity.md)
- [current_common_views_fai_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/current_common_views_fai_identity.md)
- [sync_import_version_relation](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_conflicts/sync_import_version_relation.md)
- [allocate_output_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/allocate_output_handle.md)
- [set_handle_slot](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/set_handle_slot.md)
- [rop_synchronization_import_message_change_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/responses/rop_synchronization_import_message_change_response.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [apply_pending_associated_message_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/apply_pending_associated_message_property_values.md)
- [event_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/event_for_id.md)
- [imported_event_identity_from_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/imported_event_identity_from_properties.md)
- [imported_event_last_modification_filetime](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/imported_event_last_modification_filetime.md)
- [imported_event_transaction](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/imported_event_transaction.md)
- [change_number_for_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id.md)
- [import_message_change_conflicts_with_current_pcl](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/import_message_change_conflicts_with_current_pcl.md)
- [apply_canonical_message_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/apply_canonical_message_property_values.md)
- [record_sync_upload_content_change](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/record_sync_upload_content_change.md)
- [public_folder_item_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_item_for_id.md)
- [apply_canonical_public_folder_item_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/apply_canonical_public_folder_item_property_values.md)
- [note_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/note_for_id.md)
- [apply_canonical_note_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/notes/apply_canonical_note_property_values.md)
- [journal_entry_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/journal_entry_for_id.md)
- [apply_canonical_journal_entry_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/notes/apply_canonical_journal_entry_property_values.md)
- [collaboration_folder_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id.md)
- [imported_contact_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_message/imported_contact_identity.md)

# Called by

- [append_sync_import_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_sync_import_dispatch_response.md)
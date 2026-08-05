---
type: Rust Module
title: sync_import
resource: crates/lpe-exchange/src/mapi/dispatch/sync_import.rs#L1-L1443
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [first_fast_transfer_marker](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/first_fast_transfer_marker.md)
- [fast_transfer_destination_target_folder_id](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/fast_transfer_destination_target_folder_id.md)
- [staged_fast_transfer_destination_buffer](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/staged_fast_transfer_destination_buffer.md)
- [commit_fast_transfer_destination_buffer](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/commit_fast_transfer_destination_buffer.md)
- [append_tell_version_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_tell_version_response.md)
- [is_sync_import_rop](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/is_sync_import_rop.md)
- [append_sync_import_dispatch_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_sync_import_dispatch_response.md)
- [append_fast_transfer_source_copy_messages_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_fast_transfer_source_copy_messages_response.md)
- [append_fast_transfer_source_copy_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_fast_transfer_source_copy_response.md)
- [append_synchronization_get_transfer_state_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_synchronization_get_transfer_state_response.md)
- [append_fast_transfer_destination_configure_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_fast_transfer_destination_configure_response.md)
- [append_synchronization_open_collector_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_synchronization_open_collector_response.md)
- [append_fast_transfer_destination_put_buffer_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_fast_transfer_destination_put_buffer_response.md)
- [apply_fast_transfer_destination_properties](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/apply_fast_transfer_destination_properties.md)
- [fast_transfer_property_values](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/fast_transfer_property_values.md)
- [read_fast_transfer_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/read_fast_transfer_property_value.md)
- [read_fast_transfer_variable_bytes](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/read_fast_transfer_variable_bytes.md)
- [decode_fast_transfer_string8](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/decode_fast_transfer_string8.md)
- [decode_fast_transfer_utf16](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/decode_fast_transfer_utf16.md)
- [imported_property_source_key_global_counter](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/imported_property_source_key_global_counter.md)
- [imported_message_source_key](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/imported_message_source_key.md)
- [import_message_change_conflicts_with_current_pcl](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/import_message_change_conflicts_with_current_pcl.md)
- [PredecessorChangeListEntry](../../../../../../classes/crates/lpe-exchange/src/mapi/dispatch/sync_import/PredecessorChangeListEntry.md)
- [parse_predecessor_change_list_entries](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/parse_predecessor_change_list_entries.md)
- [persistable_import_source_key_global_counter](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/persistable_import_source_key_global_counter.md)
- [source_key_global_counter](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/source_key_global_counter.md)
- [import_source_key_identity_scope](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/import_source_key_identity_scope.md)
- [pending_message_is_sync_metadata_only](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/pending_message_is_sync_metadata_only.md)
- [pending_message_is_trash_sync_artifact](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/pending_message_is_trash_sync_artifact.md)
- [imported_hierarchy_parent_mailbox_id](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/imported_hierarchy_parent_mailbox_id.md)
- [hierarchy_checkpoint_status](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/hierarchy_checkpoint_status.md)
- [sync_property_filter_mode](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/sync_property_filter_mode.md)
- [upload_state_property_name](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/upload_state_property_name.md)
- [upload_state_marker_bit](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/upload_state_marker_bit.md)
- [uploaded_state_has_delta_anchor](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/uploaded_state_has_delta_anchor.md)
- [mark_uploaded_state_stream](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/mark_uploaded_state_stream.md)
- [record_sync_upload_content_change](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/record_sync_upload_content_change.md)
- [record_sync_upload_content_checkpoint](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/record_sync_upload_content_checkpoint.md)
- [record_sync_upload_hierarchy_change_with_change_number](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/record_sync_upload_hierarchy_change_with_change_number.md)
- [sync_mailboxes_with_collaboration_counts](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/sync_mailboxes_with_collaboration_counts.md)
- [collaboration_folder_in_hierarchy_sync_scope](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/collaboration_folder_in_hierarchy_sync_scope.md)
- [mapi_message_ids_for_deleted_changes](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/mapi_message_ids_for_deleted_changes.md)
- [mapi_object_ids_for_deleted_changes](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/mapi_object_ids_for_deleted_changes.md)
- [changed_special_ids_for_folder](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/changed_special_ids_for_folder.md)
- [deleted_special_object_ids_for_folder](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/deleted_special_object_ids_for_folder.md)
- [remember_created_mapi_identity](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/remember_created_mapi_identity.md)
- [remember_created_mapi_identity_record](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/remember_created_mapi_identity_record.md)
- [remember_created_message_mapi_identity](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/remember_created_message_mapi_identity.md)

# Imports

- `super::*`

# Member of

- [lpe-exchange](../../../../../../packages/crates/lpe-exchange.md)
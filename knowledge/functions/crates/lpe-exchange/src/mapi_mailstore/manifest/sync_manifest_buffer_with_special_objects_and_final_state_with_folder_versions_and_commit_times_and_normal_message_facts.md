---
type: Rust Function
title: sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts
resource: crates/lpe-exchange/src/mapi_mailstore/manifest.rs#L765-L1539
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/hierarchy_sort_depth
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/hierarchy_folder_sort_order
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_id_for_mailbox
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_parent_id_for_mailbox
  - functions/crates/lpe-exchange/src/mapi_mailstore/canonical_hierarchy_change_number
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_change_number
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/hierarchy_parent_source_key_role
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_message_class
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/folder_content_counts
  - functions/crates/lpe-exchange/src/mapi_mailstore/local_commit_time_max
  - functions/crates/lpe-exchange/src/mapi_mailstore/property_tag_excluded
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_display_name
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_binary_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_i64
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/hierarchy_entry_id_mailbox_guid
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_utf16_property
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_i32_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/mapi_folder_type
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_bool_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_has_subfolders
  - functions/crates/lpe-exchange/src/mapi_mailstore/content_sync_includes_normal
  - functions/crates/lpe-exchange/src/mapi_mailstore/default_content_sync_includes_associated
  - functions/crates/lpe-exchange/src/mapi_mailstore/content_sync_includes_associated
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_content_sync_progress_mode
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/email_delivery_time
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/normal_message_sync_fact_for
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/normal_message_sync_source_key_for_fact
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_content_sync_progress_per_message
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_change_number
  - functions/crates/lpe-exchange/src/mapi_mailstore/content_property_in_scope
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_i32
  - functions/crates/lpe-exchange/src/mapi_mailstore/canonical_message_flags
  - functions/crates/lpe-exchange/src/mapi_mailstore/canonical_flag_status
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/fast_transfer_sender_name
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/fast_transfer_sender_address
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/fast_transfer_sent_representing_name
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/normalized_subject_tag
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/write_normalized_subject_property
  - functions/crates/lpe-exchange/src/mapi/properties/message/message_class_for_email
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_fast_transfer_message_children
  - functions/crates/lpe-exchange/src/mapi_mailstore/content_sync_message_children
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/special_message_delivery_sort_time
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_change_number
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_sync_source_key
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_sync_parent_source_key
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_change_key
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_predecessor_change_list
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_search_key
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_access
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_access_level
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_has_attachments
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_status
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_flags
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_property_is_ics_identity
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_property_is_server_projected
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/write_special_message_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/replid_idset_from_object_ids
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_state_token_with_special_objects_and_normal_message_facts
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/sync_manifest_serializes_content_message_header_in_fixed_order
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_download_selection_emits_unseen_durable_inbox_change_after_completed_sync
---

# Signature

`pub(crate) fn sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts( mailbox_guid: Uuid, sync_type: u8, sync_flags: u16, sync_extra_flags: u32, sync_property_tags: &[u32], folder_id: u64, mailboxes: &[JmapMailbox], emails: &[JmapEmail], attachment_facts: &[MessageAttachmentSyncFacts], normal_message_facts: &[NormalMessageSyncFact], special_objects: &[SpecialMessageSyncFact], deleted_message_ids: &[u64], parent_context_mailboxes: &[JmapMailbox], state_mailboxes: &[JmapMailbox], state_emails: &[JmapEmail], state_attachment_facts: &[MessageAttachmentSyncFacts], state_normal_message_facts: &[NormalMessageSyncFact], state_special_objects: &[SpecialMessageSyncFact], aggregate_emails: &[JmapEmail], aggregate_attachment_facts: &[MessageAttachmentSyncFacts], folder_versions: &[crate::mapi_store::MapiFolderVersion], folder_commit_times: &[(u64, u64)], _final_change_sequence: u64, ) -> Vec<u8>`

# Calls

- [hierarchy_sort_depth](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/hierarchy_sort_depth.md)
- [hierarchy_folder_sort_order](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/hierarchy_folder_sort_order.md)
- [mapi_folder_id_for_mailbox](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_id_for_mailbox.md)
- [mapi_folder_parent_id_for_mailbox](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_parent_id_for_mailbox.md)
- [canonical_hierarchy_change_number](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/canonical_hierarchy_change_number.md)
- [filetime_from_change_number](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_change_number.md)
- [source_key_for_store_id](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id.md)
- [hierarchy_parent_source_key_role](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/diagnostics/hierarchy_parent_source_key_role.md)
- [mapi_folder_message_class](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_message_class.md)
- [folder_content_counts](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/folder_content_counts.md)
- [local_commit_time_max](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/local_commit_time_max.md)
- [property_tag_excluded](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/property_tag_excluded.md)
- [mapi_folder_display_name](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_display_name.md)
- [write_binary_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_binary_property.md)
- [write_i64](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_i64.md)
- [hierarchy_entry_id_mailbox_guid](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/hierarchy_entry_id_mailbox_guid.md)
- [write_utf16_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_utf16_property.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [write_i32_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_i32_property.md)
- [mapi_folder_type](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/mapi_folder_type.md)
- [write_bool_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_bool_property.md)
- [mapi_folder_has_subfolders](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/folders/mapi_folder_has_subfolders.md)
- [content_sync_includes_normal](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/content_sync_includes_normal.md)
- [default_content_sync_includes_associated](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/default_content_sync_includes_associated.md)
- [content_sync_includes_associated](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/content_sync_includes_associated.md)
- [write_content_sync_progress_mode](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_content_sync_progress_mode.md)
- [email_delivery_time](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/email_delivery_time.md)
- [normal_message_sync_fact_for](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/normal_message_sync_fact_for.md)
- [normal_message_sync_source_key_for_fact](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/normal_message_sync_source_key_for_fact.md)
- [write_content_sync_progress_per_message](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_content_sync_progress_per_message.md)
- [write_change_number](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_change_number.md)
- [content_property_in_scope](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/content_property_in_scope.md)
- [write_i32](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_i32.md)
- [canonical_message_flags](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/canonical_message_flags.md)
- [canonical_flag_status](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/canonical_flag_status.md)
- [fast_transfer_sender_name](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/fast_transfer_sender_name.md)
- [fast_transfer_sender_address](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/fast_transfer_sender_address.md)
- [fast_transfer_sent_representing_name](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/fast_transfer_sent_representing_name.md)
- [normalized_subject_tag](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/normalized_subject_tag.md)
- [write_normalized_subject_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/write_normalized_subject_property.md)
- [message_class_for_email](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/message_class_for_email.md)
- [write_fast_transfer_message_children](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_fast_transfer_message_children.md)
- [content_sync_message_children](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/content_sync_message_children.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [special_message_delivery_sort_time](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/special_message_delivery_sort_time.md)
- [special_message_change_number](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_change_number.md)
- [special_message_sync_source_key](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_sync_source_key.md)
- [special_message_sync_parent_source_key](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_sync_parent_source_key.md)
- [special_message_change_key](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_change_key.md)
- [special_message_predecessor_change_list](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_predecessor_change_list.md)
- [special_message_search_key](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_search_key.md)
- [special_message_access](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_access.md)
- [special_message_access_level](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_access_level.md)
- [special_message_has_attachments](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_has_attachments.md)
- [special_message_status](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_status.md)
- [special_message_flags](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_flags.md)
- [special_message_property_is_ics_identity](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_property_is_ics_identity.md)
- [special_message_property_is_server_projected](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_property_is_server_projected.md)
- [write_special_message_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/write_special_message_property.md)
- [replid_idset_from_object_ids](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/replid_idset_from_object_ids.md)
- [sync_state_token_with_special_objects_and_normal_message_facts](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_state_token_with_special_objects_and_normal_message_facts.md)

# Called by

- [append_synchronization_configure_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response.md)
- [sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times.md)
- [sync_manifest_serializes_content_message_header_in_fixed_order](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/sync_manifest_serializes_content_message_header_in_fixed_order.md)
- [content_download_selection_emits_unseen_durable_inbox_change_after_completed_sync](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_download_selection_emits_unseen_durable_inbox_change_after_completed_sync.md)
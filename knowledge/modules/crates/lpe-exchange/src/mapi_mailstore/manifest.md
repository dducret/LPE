---
type: Rust Module
title: manifest
resource: crates/lpe-exchange/src/mapi_mailstore/manifest.rs#L1-L1465
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super-special-message-special-message-access-special-message-access-level-special-message-change-key-special-message-change-number-special-message-flags-special-message-has-attachments-special-message-parent-source-key-special-message-predecessor-change-list-special-message-property-is-ics-identity-special-message-property-is-server-projected-special-message-search-key-special-message-status-special-message-sync-source-key-write-special-message-property-pid-tag-has-attachments-pid-tag-message-status
  - external/super
  - external/crate-mapi-properties-message-class-for-email
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [AttachmentSyncFact](../../../../../classes/crates/lpe-exchange/src/mapi_mailstore/manifest/AttachmentSyncFact.md)
- [MessageAttachmentSyncFacts](../../../../../classes/crates/lpe-exchange/src/mapi_mailstore/manifest/MessageAttachmentSyncFacts.md)
- [FaiContentSyncDebugContext](../../../../../classes/crates/lpe-exchange/src/mapi_mailstore/manifest/FaiContentSyncDebugContext.md)
- [default](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/FaiContentSyncDebugContext/default/default.md)
- [normalized_subject_tag](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/normalized_subject_tag.md)
- [email_delivery_time](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/email_delivery_time.md)
- [fast_transfer_sender_name](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/fast_transfer_sender_name.md)
- [fast_transfer_sender_address](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/fast_transfer_sender_address.md)
- [fast_transfer_sent_representing_name](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/fast_transfer_sent_representing_name.md)
- [write_fast_transfer_message_content](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/write_fast_transfer_message_content.md)
- [special_message_delivery_sort_time](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/special_message_delivery_sort_time.md)
- [write_normalized_subject_property](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/write_normalized_subject_property.md)
- [canonical_folder_change_number](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/canonical_folder_change_number.md)
- [canonical_message_change_number](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/canonical_message_change_number.md)
- [canonical_message_change_number_with_attachments](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/canonical_message_change_number_with_attachments.md)
- [normal_message_sync_fact_for](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/normal_message_sync_fact_for.md)
- [normal_message_sync_source_key_for_fact](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/normal_message_sync_source_key_for_fact.md)
- [source_key_for_uuid](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_uuid.md)
- [source_key_for_store_id](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id.md)
- [source_key_for_mailbox_folder](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_mailbox_folder.md)
- [source_key_for_mailbox_role](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_mailbox_role.md)
- [virtual_special_mailbox](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/virtual_special_mailbox.md)
- [change_key_for_change_number](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_key_for_change_number.md)
- [change_number_for_store_id](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id.md)
- [predecessor_change_list](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/predecessor_change_list.md)
- [filetime_from_rfc3339_utc](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_rfc3339_utc.md)
- [filetime_from_change_number](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_change_number.md)
- [parse_rfc3339_utc_filetime](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/parse_rfc3339_utc_filetime.md)
- [days_in_month](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/days_in_month.md)
- [parse_digits](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/parse_digits.md)
- [sync_state_token_with_attachments](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_state_token_with_attachments.md)
- [sync_state_token_with_special_objects_and_normal_message_facts](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_state_token_with_special_objects_and_normal_message_facts.md)
- [sync_manifest_buffer_with_attachments](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_attachments.md)
- [sync_manifest_buffer_with_final_state](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_final_state.md)
- [sync_manifest_buffer_with_special_objects_and_final_state](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state.md)
- [sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions.md)
- [sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times.md)
- [sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts.md)

# Imports

- `super::special_message::{
    special_message_access, special_message_access_level, special_message_change_key,
    special_message_change_number, special_message_flags, special_message_has_attachments,
    special_message_parent_source_key, special_message_predecessor_change_list,
    special_message_property_is_ics_identity, special_message_property_is_server_projected,
    special_message_search_key, special_message_status, special_message_sync_source_key,
    write_special_message_property, PID_TAG_HAS_ATTACHMENTS, PID_TAG_MESSAGE_STATUS,
}`
- `super::*`
- `crate::mapi::properties::message_class_for_email`

# Member of

- [lpe-exchange](../../../../../packages/crates/lpe-exchange.md)
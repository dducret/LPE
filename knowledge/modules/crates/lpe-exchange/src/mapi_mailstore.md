---
type: Rust Module
title: mapi_mailstore
resource: crates/lpe-exchange/src/mapi_mailstore.rs#L1-L1490
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/std-collections-btreeset-hashmap
  - external/lpe-domain-crypto-hex-lower-days-from-civil-windows-filetime-from-signed-unix-seconds-windows-filetime-ticks-per-second-windows-unix-epoch-offset-seconds
  - external/lpe-storage-jmapemail-jmapmailbox
  - external/uuid-uuid
  - external/pub-crate-use-client-state-download-change-facts
  - external/pub-crate-use-client-state-download-change-facts-with-normal-message-sync-facts-select-download-manifest-for-client-state-validate-download-state-property-downloadchangefact
  - external/pub-crate-use-folders
  - external/pub-crate-use-manifest-sync-manifest-buffer-with-attachments
  - external/pub-crate-use-manifest-sync-manifest-buffer-with-final-state
  - external/pub-crate-use-manifest-canonical-folder-change-number-canonical-message-change-number-canonical-message-change-number-with-attachments-change-key-for-change-number-change-number-for-store-id-filetime-from-change-number-filetime-from-rfc3339-utc-predecessor-change-list-source-key-for-mailbox-folder-source-key-for-mailbox-role-source-key-for-store-id-source-key-for-uuid-sync-manifest-buffer-with-special-objects-and-final-state-with-folder-versions-and-commit-times-and-normal-message-facts-sync-state-token-with-attachments-sync-state-token-with-special-objects-and-normal-message-facts-virtual-special-mailbox-attachmentsyncfact-faicontentsyncdebugcontext-messageattachmentsyncfacts
  - external/pub-crate-use-manifest-sync-manifest-buffer-with-special-objects-and-final-state-sync-manifest-buffer-with-special-objects-and-final-state-with-folder-versions
  - external/pub-crate-use-special-message-fast-transfer-message-content-buffer-with-special-object-special-message-change-key-special-message-change-number-special-message-predecessor-change-list-special-message-source-key-special-message-sync-parent-source-key-special-message-sync-source-key-specialmessagepropertyvalue-specialmessagesyncfact
  - external/pub-crate-use-diagnostics-decode-content-transfer-fai-debug-summary-decode-hierarchy-transfer-debug-summary-fai-debug-state-origin-format-marker-tags-hierarchy-identity-properties-before-display-name-hierarchy-microsoft-payload-comparison-hierarchy-semantic-validation-contenttransferfaidebugsummary-contenttransferfaiitemdebug
  - external/pub-crate-use-diagnostics-default-folder-hierarchy-membership-summary-final-sync-state-debug-summary-hierarchy-parent-source-key-role-hierarchy-transfer-close-summary-log-fai-content-sync-debug-log-hierarchy-get-buffer-payload-summary-log-hierarchy-transfer-debug-replguid-globset-counters-replguid-globset-debug-summary
  - external/pub-crate-use-crate-mapi-identity-store-replica-guid
  - external/crate-mapi-properties-canonical-property-storage-tag
  - external/crate-mapi-wire-fasttransfermarker-mapisynctype
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [NormalMessageSyncFact](../../../../classes/crates/lpe-exchange/src/mapi_mailstore/NormalMessageSyncFact.md)
- [FastTransferMessageChildren](../../../../classes/crates/lpe-exchange/src/mapi_mailstore/FastTransferMessageChildren.md)
- [new](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/FastTransferMessageChildren/new.md)
- [all](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/FastTransferMessageChildren/all.md)
- [FastTransferDirectPropertyFilter](../../../../classes/crates/lpe-exchange/src/mapi_mailstore/FastTransferDirectPropertyFilter.md)
- [for_rop](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/FastTransferDirectPropertyFilter/for_rop.md)
- [includes](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/FastTransferDirectPropertyFilter/includes.md)
- [fast_transfer_property_included](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_property_included.md)
- [property_tag_excluded](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/property_tag_excluded.md)
- [provider_defined_internal_property](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/provider_defined_internal_property.md)
- [property_tag_matches](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/property_tag_matches.md)
- [property_tag_requested](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/property_tag_requested.md)
- [content_property_in_scope](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/content_property_in_scope.md)
- [content_sync_message_children](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/content_sync_message_children.md)
- [content_sync_includes_normal](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/content_sync_includes_normal.md)
- [default_content_sync_includes_associated](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/default_content_sync_includes_associated.md)
- [content_sync_includes_associated](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/content_sync_includes_associated.md)
- [mapi_folder_type](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/mapi_folder_type.md)
- [local_commit_time_max](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/local_commit_time_max.md)
- [sync_state_object_ids](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/sync_state_object_ids.md)
- [sync_state_change_numbers](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/sync_state_change_numbers.md)
- [canonical_hierarchy_change_number](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/canonical_hierarchy_change_number.md)
- [final_sync_state_stream](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/final_sync_state_stream.md)
- [initial_sync_state_stream](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/initial_sync_state_stream.md)
- [sync_state_stream_with_uploaded_property](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/sync_state_stream_with_uploaded_property.md)
- [upload_sync_state_stream_with_uploaded_property](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/upload_sync_state_stream_with_uploaded_property.md)
- [upload_sync_state_stream_from_sets](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/upload_sync_state_stream_from_sets.md)
- [final_content_sync_state_stream](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/final_content_sync_state_stream.md)
- [sync_state_stream_from_raw_properties](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/sync_state_stream_from_raw_properties.md)
- [upload_sync_state_stream_from_raw_properties](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/upload_sync_state_stream_from_raw_properties.md)
- [final_sync_state_stream_with_cnsets](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/final_sync_state_stream_with_cnsets.md)
- [sync_state_property_value](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/sync_state_property_value.md)
- [read_sync_state_u32](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/read_sync_state_u32.md)
- [fast_transfer_manifest_buffer_with_attachments](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_manifest_buffer_with_attachments.md)
- [fast_transfer_message_list_buffer_with_attachments](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_message_list_buffer_with_attachments.md)
- [fast_transfer_message_content_buffer_with_attachments](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_message_content_buffer_with_attachments.md)
- [write_fast_transfer_message_children](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_fast_transfer_message_children.md)
- [fast_transfer_top_folder_buffer_with_attachments](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_top_folder_buffer_with_attachments.md)
- [write_fast_transfer_folder_content](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_fast_transfer_folder_content.md)
- [write_fast_transfer_folder_properties](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_fast_transfer_folder_properties.md)
- [fast_transfer_child_mailboxes](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_child_mailboxes.md)
- [fast_transfer_emails_for_folder](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_emails_for_folder.md)
- [fast_transfer_email_matches_folder](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_email_matches_folder.md)
- [canonical_message_flags](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/canonical_message_flags.md)
- [canonical_message_flags_for_state](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/canonical_message_flags_for_state.md)
- [canonical_flag_status](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/canonical_flag_status.md)
- [write_visible_recipient_facts](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_visible_recipient_facts.md)
- [write_fast_transfer_visible_recipients](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_fast_transfer_visible_recipients.md)
- [write_fast_transfer_attachments](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_fast_transfer_attachments.md)
- [attachment_sync_fact_is_embedded_message](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/attachment_sync_fact_is_embedded_message.md)
- [write_fast_transfer_embedded_message](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_fast_transfer_embedded_message.md)
- [EmbeddedMessageFastTransferProperties](../../../../classes/crates/lpe-exchange/src/mapi_mailstore/EmbeddedMessageFastTransferProperties.md)
- [embedded_message_properties_from_attachment](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/embedded_message_properties_from_attachment.md)
- [embedded_message_properties_from_blob](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/embedded_message_properties_from_blob.md)
- [embedded_message_subject_from_file_name](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/embedded_message_subject_from_file_name.md)
- [attachments_for_message](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/attachments_for_message.md)
- [canonical_modseq_change_number](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/canonical_modseq_change_number.md)
- [write_prefixed_bytes](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_prefixed_bytes.md)
- [write_u32](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_u32.md)
- [write_i32](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_i32.md)
- [write_i64](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_i64.md)
- [write_object_id](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_object_id.md)
- [write_change_number](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_change_number.md)
- [write_i32_property](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_i32_property.md)
- [write_content_sync_progress_mode](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_content_sync_progress_mode.md)
- [write_content_sync_progress_per_message](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_content_sync_progress_per_message.md)
- [write_bool_property](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_bool_property.md)
- [write_binary_property](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_binary_property.md)
- [write_utf16_property](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_utf16_property.md)
- [write_string8_property](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_string8_property.md)
- [fast_transfer_property_value_start](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/fast_transfer_property_value_start.md)
- [replguid_idset_from_object_ids](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/replguid_idset_from_object_ids.md)
- [replguid_idset_from_counters](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/replguid_idset_from_counters.md)
- [replid_idset_from_object_ids](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/replid_idset_from_object_ids.md)
- [coalesced_ranges](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/coalesced_ranges.md)
- [write_globset_ranges](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_globset_ranges.md)
- [globcnt_bytes](../../../../functions/crates/lpe-exchange/src/mapi_mailstore/globcnt_bytes.md)

# Imports

- `std::collections::{BTreeSet, HashMap}`
- `lpe_domain::{
    crypto::hex_lower, days_from_civil, windows_filetime_from_signed_unix_seconds,
    WINDOWS_FILETIME_TICKS_PER_SECOND, WINDOWS_UNIX_EPOCH_OFFSET_SECONDS,
}`
- `lpe_storage::{JmapEmail, JmapMailbox}`
- `uuid::Uuid`
- `pub(crate) use client_state::download_change_facts`
- `pub(crate) use client_state::{
    download_change_facts_with_normal_message_sync_facts,
    select_download_manifest_for_client_state, validate_download_state_property,
    DownloadChangeFact,
}`
- `pub(crate) use folders::*`
- `pub(crate) use manifest::sync_manifest_buffer_with_attachments`
- `pub(crate) use manifest::sync_manifest_buffer_with_final_state`
- `pub(crate) use manifest::{
    canonical_folder_change_number, canonical_message_change_number,
    canonical_message_change_number_with_attachments, change_key_for_change_number,
    change_number_for_store_id, filetime_from_change_number, filetime_from_rfc3339_utc,
    predecessor_change_list, source_key_for_mailbox_folder, source_key_for_mailbox_role,
    source_key_for_store_id, source_key_for_uuid,
    sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions_and_commit_times_and_normal_message_facts,
    sync_state_token_with_attachments,
    sync_state_token_with_special_objects_and_normal_message_facts, virtual_special_mailbox,
    AttachmentSyncFact, FaiContentSyncDebugContext, MessageAttachmentSyncFacts,
}`
- `pub(crate) use manifest::{
    sync_manifest_buffer_with_special_objects_and_final_state,
    sync_manifest_buffer_with_special_objects_and_final_state_with_folder_versions,
}`
- `pub(crate) use special_message::{
    fast_transfer_message_content_buffer_with_special_object, special_message_change_key,
    special_message_change_number, special_message_predecessor_change_list,
    special_message_source_key, special_message_sync_parent_source_key,
    special_message_sync_source_key, SpecialMessagePropertyValue, SpecialMessageSyncFact,
}`
- `pub(crate) use diagnostics::{
    decode_content_transfer_fai_debug_summary, decode_hierarchy_transfer_debug_summary,
    fai_debug_state_origin, format_marker_tags, hierarchy_identity_properties_before_display_name,
    hierarchy_microsoft_payload_comparison, hierarchy_semantic_validation,
    ContentTransferFaiDebugSummary, ContentTransferFaiItemDebug,
}`
- `pub(crate) use diagnostics::{
    default_folder_hierarchy_membership_summary, final_sync_state_debug_summary,
    hierarchy_parent_source_key_role, hierarchy_transfer_close_summary, log_fai_content_sync_debug,
    log_hierarchy_get_buffer_payload_summary, log_hierarchy_transfer_debug,
    replguid_globset_counters, replguid_globset_debug_summary,
}`
- `pub(crate) use crate::mapi::identity::STORE_REPLICA_GUID`
- `crate::mapi::properties::canonical_property_storage_tag`
- `crate::mapi::wire::{FastTransferMarker, MapiSyncType}`

# Member of

- [lpe-exchange](../../../../packages/crates/lpe-exchange.md)
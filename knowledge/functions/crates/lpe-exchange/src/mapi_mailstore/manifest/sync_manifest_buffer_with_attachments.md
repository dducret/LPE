---
type: Rust Function
title: sync_manifest_buffer_with_attachments
resource: crates/lpe-exchange/src/mapi_mailstore/manifest.rs#L543-L574
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_final_state
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_state_keeps_deleted_items_fid_distinct_from_its_server_cn
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/sync_manifest_serializes_variable_strings_with_fast_transfer_lengths
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_content_sync_emits_sender_and_delivery_identity_properties
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_order_by_delivery_time_sorts_newest_message_changes_first
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_content_sync_uses_recipient_markers
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_content_sync_uses_attachment_markers
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_content_sync_uses_embedded_message_markers
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_keeps_subfolders_optional_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/root_hierarchy_transfer_ipm_subtree_reports_virtual_children
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_debug_decoder_summarizes_serialized_stream
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/ipm_hierarchy_transfer_excludes_sync_root_and_zeros_direct_child_parent_key
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_selection_uses_uploaded_empty_client_state
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_selection_preserves_foreign_replica_and_uses_local_cnset
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_no_deletions_keeps_missing_id_without_tombstone
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_emits_explicit_tombstone_absent_from_client_idset
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_rejects_malformed_client_globset
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_microsoft_payload_comparison_matches_documented_folder_change_rules
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_debug_summary_tracks_emitted_ipm_final_state_counters
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/default_folder_hierarchy_membership_summary_tracks_top_level_ipm_folders
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_without_eid_omits_folder_id_but_keeps_parent_folder_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_includes_folder_id_with_eid_extra_flag
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_omits_custom_sync_root_and_projects_children
  - functions/crates/lpe-exchange/src/mapi_mailstore/tests/normal_message_no_foreign_identifiers_uses_local_source_key_for_selection
---

# Signature

`pub(crate) fn sync_manifest_buffer_with_attachments( sync_type: u8, sync_flags: u16, sync_extra_flags: u32, sync_property_tags: &[u32], folder_id: u64, mailboxes: &[JmapMailbox], emails: &[JmapEmail], attachment_facts: &[MessageAttachmentSyncFacts], deleted_message_ids: &[u64], final_change_sequence: u64, ) -> Vec<u8>`

# Calls

- [sync_manifest_buffer_with_final_state](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/sync_manifest_buffer_with_final_state.md)

# Called by

- [hierarchy_state_keeps_deleted_items_fid_distinct_from_its_server_cn](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_state_keeps_deleted_items_fid_distinct_from_its_server_cn.md)
- [sync_manifest_serializes_variable_strings_with_fast_transfer_lengths](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/sync_manifest_serializes_variable_strings_with_fast_transfer_lengths.md)
- [microsoft_oxcfxics_content_sync_emits_sender_and_delivery_identity_properties](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_content_sync_emits_sender_and_delivery_identity_properties.md)
- [microsoft_oxcfxics_order_by_delivery_time_sorts_newest_message_changes_first](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_order_by_delivery_time_sorts_newest_message_changes_first.md)
- [microsoft_oxcfxics_content_sync_uses_recipient_markers](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_content_sync_uses_recipient_markers.md)
- [microsoft_oxcfxics_content_sync_uses_attachment_markers](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_content_sync_uses_attachment_markers.md)
- [microsoft_oxcfxics_content_sync_uses_embedded_message_markers](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_content_sync_uses_embedded_message_markers.md)
- [hierarchy_transfer_keeps_subfolders_optional_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_keeps_subfolders_optional_property.md)
- [root_hierarchy_transfer_ipm_subtree_reports_virtual_children](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/root_hierarchy_transfer_ipm_subtree_reports_virtual_children.md)
- [hierarchy_transfer_debug_decoder_summarizes_serialized_stream](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_debug_decoder_summarizes_serialized_stream.md)
- [ipm_hierarchy_transfer_excludes_sync_root_and_zeros_direct_child_parent_key](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/ipm_hierarchy_transfer_excludes_sync_root_and_zeros_direct_child_parent_key.md)
- [hierarchy_download_selection_uses_uploaded_empty_client_state](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_selection_uses_uploaded_empty_client_state.md)
- [hierarchy_download_selection_preserves_foreign_replica_and_uses_local_cnset](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_selection_preserves_foreign_replica_and_uses_local_cnset.md)
- [hierarchy_download_no_deletions_keeps_missing_id_without_tombstone](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_no_deletions_keeps_missing_id_without_tombstone.md)
- [hierarchy_download_emits_explicit_tombstone_absent_from_client_idset](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_emits_explicit_tombstone_absent_from_client_idset.md)
- [hierarchy_download_rejects_malformed_client_globset](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_rejects_malformed_client_globset.md)
- [hierarchy_microsoft_payload_comparison_matches_documented_folder_change_rules](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_microsoft_payload_comparison_matches_documented_folder_change_rules.md)
- [hierarchy_transfer_debug_summary_tracks_emitted_ipm_final_state_counters](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_debug_summary_tracks_emitted_ipm_final_state_counters.md)
- [default_folder_hierarchy_membership_summary_tracks_top_level_ipm_folders](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/default_folder_hierarchy_membership_summary_tracks_top_level_ipm_folders.md)
- [hierarchy_transfer_without_eid_omits_folder_id_but_keeps_parent_folder_id](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_without_eid_omits_folder_id_but_keeps_parent_folder_id.md)
- [hierarchy_transfer_includes_folder_id_with_eid_extra_flag](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_includes_folder_id_with_eid_extra_flag.md)
- [hierarchy_transfer_omits_custom_sync_root_and_projects_children](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_omits_custom_sync_root_and_projects_children.md)
- [normal_message_no_foreign_identifiers_uses_local_source_key_for_selection](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/normal_message_no_foreign_identifiers_uses_local_source_key_for_selection.md)
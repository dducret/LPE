---
type: Rust Module
title: tests
resource: crates/lpe-exchange/src/mapi_mailstore/tests.rs#L1-L4388
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super
  - external/lpe-storage-jmapemailaddress-jmapemailmailboxstate
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [property_filters_match_ptyp_unspecified_by_property_id](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/property_filters_match_ptyp_unspecified_by_property_id.md)
- [content_sync_child_collections_follow_property_filters_independently](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_sync_child_collections_follow_property_filters_independently.md)
- [wire_id_bytes](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/wire_id_bytes.md)
- [rfc3339_filetime_accepts_postgresql_microseconds_and_preserves_100ns_ticks](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/rfc3339_filetime_accepts_postgresql_microseconds_and_preserves_100ns_ticks.md)
- [message_change_number_excludes_bcc_recipients](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/message_change_number_excludes_bcc_recipients.md)
- [message_change_number_tracks_per_folder_membership_state](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/message_change_number_tracks_per_folder_membership_state.md)
- [canonical_change_numbers_fit_mapi_globcnt](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/canonical_change_numbers_fit_mapi_globcnt.md)
- [source_and_change_keys_are_stable_replica_scoped_values](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/source_and_change_keys_are_stable_replica_scoped_values.md)
- [store_id_change_numbers_use_global_counter](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/store_id_change_numbers_use_global_counter.md)
- [hierarchy_state_keeps_deleted_items_fid_distinct_from_its_server_cn](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_state_keeps_deleted_items_fid_distinct_from_its_server_cn.md)
- [hierarchy_download_keeps_imported_change_key_and_predecessor_lineage](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_keeps_imported_change_key_and_predecessor_lineage.md)
- [hierarchy_change_numbers_use_distinct_persisted_folder_versions](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_change_numbers_use_distinct_persisted_folder_versions.md)
- [hierarchy_change_number_uses_projected_mailbox_modseq](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_change_number_uses_projected_mailbox_modseq.md)
- [special_folder_source_key_matches_projected_folder_id](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/special_folder_source_key_matches_projected_folder_id.md)
- [predecessor_change_list_uses_sized_change_xid](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/predecessor_change_list_uses_sized_change_xid.md)
- [unchanged_object_keeps_source_key_and_changed_object_advances_change_number](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/unchanged_object_keeps_source_key_and_changed_object_advances_change_number.md)
- [canonical_message_change_number_uses_membership_modseq_without_bcc_leakage](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/canonical_message_change_number_uses_membership_modseq_without_bcc_leakage.md)
- [sync_manifest_serializes_variable_strings_with_fast_transfer_lengths](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/sync_manifest_serializes_variable_strings_with_fast_transfer_lengths.md)
- [sync_manifest_serializes_content_message_header_in_fixed_order](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/sync_manifest_serializes_content_message_header_in_fixed_order.md)
- [content_download_selection_emits_unseen_durable_inbox_change_after_completed_sync](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_download_selection_emits_unseen_durable_inbox_change_after_completed_sync.md)
- [microsoft_oxcfxics_content_sync_emits_sender_and_delivery_identity_properties](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_content_sync_emits_sender_and_delivery_identity_properties.md)
- [microsoft_oxcfxics_order_by_delivery_time_sorts_newest_message_changes_first](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_order_by_delivery_time_sorts_newest_message_changes_first.md)
- [microsoft_oxcfxics_order_by_delivery_time_interleaves_normal_and_fai_changes](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_order_by_delivery_time_interleaves_normal_and_fai_changes.md)
- [microsoft_oxcfxics_order_by_delivery_time_uses_calendar_delivery_property](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_order_by_delivery_time_uses_calendar_delivery_property.md)
- [microsoft_oxcfxics_content_sync_uses_recipient_markers](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_content_sync_uses_recipient_markers.md)
- [microsoft_oxcfxics_content_sync_uses_attachment_markers](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_content_sync_uses_attachment_markers.md)
- [microsoft_oxcfxics_content_sync_uses_embedded_message_markers](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_content_sync_uses_embedded_message_markers.md)
- [microsoft_oxcfxics_fast_transfer_copy_messages_uses_message_markers](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_fast_transfer_copy_messages_uses_message_markers.md)
- [fast_transfer_copy_properties_filters_message_identity_properties](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/fast_transfer_copy_properties_filters_message_identity_properties.md)
- [direct_fast_transfer_uses_persisted_normal_message_identity_properties](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/direct_fast_transfer_uses_persisted_normal_message_identity_properties.md)
- [microsoft_oxcfxics_fast_transfer_copy_fai_uses_message_content_root](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_fast_transfer_copy_fai_uses_message_content_root.md)
- [outlook_fai_copyto_generates_a_mapiuid_search_key](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/outlook_fai_copyto_generates_a_mapiuid_search_key.md)
- [microsoft_oxcfxics_fast_transfer_copy_folder_uses_top_folder_markers](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_fast_transfer_copy_folder_uses_top_folder_markers.md)
- [microsoft_oxcfxics_fast_transfer_copy_folder_uses_subfolder_markers](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_fast_transfer_copy_folder_uses_subfolder_markers.md)
- [hierarchy_transfer_keeps_subfolders_optional_property](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_keeps_subfolders_optional_property.md)
- [root_hierarchy_transfer_ipm_subtree_reports_virtual_children](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/root_hierarchy_transfer_ipm_subtree_reports_virtual_children.md)
- [hierarchy_transfer_debug_decoder_summarizes_serialized_stream](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_debug_decoder_summarizes_serialized_stream.md)
- [ipm_hierarchy_transfer_excludes_sync_root_and_zeros_direct_child_parent_key](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/ipm_hierarchy_transfer_excludes_sync_root_and_zeros_direct_child_parent_key.md)
- [hierarchy_download_selection_uses_uploaded_empty_client_state](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_selection_uses_uploaded_empty_client_state.md)
- [hierarchy_download_selection_preserves_foreign_replica_and_uses_local_cnset](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_selection_preserves_foreign_replica_and_uses_local_cnset.md)
- [hierarchy_download_no_deletions_keeps_missing_id_without_tombstone](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_no_deletions_keeps_missing_id_without_tombstone.md)
- [hierarchy_download_emits_explicit_tombstone_absent_from_client_idset](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_emits_explicit_tombstone_absent_from_client_idset.md)
- [hierarchy_download_rejects_malformed_client_globset](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_download_rejects_malformed_client_globset.md)
- [hierarchy_parent_source_key_role_matches_microsoft_ics_root_child_rule](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_parent_source_key_role_matches_microsoft_ics_root_child_rule.md)
- [hierarchy_microsoft_payload_comparison_matches_documented_folder_change_rules](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_microsoft_payload_comparison_matches_documented_folder_change_rules.md)
- [hierarchy_transfer_omits_targeted_optional_properties_but_keeps_required_outlook_shape](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_omits_targeted_optional_properties_but_keeps_required_outlook_shape.md)
- [hierarchy_transfer_debug_summary_tracks_emitted_ipm_final_state_counters](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_debug_summary_tracks_emitted_ipm_final_state_counters.md)
- [default_folder_hierarchy_membership_summary_tracks_top_level_ipm_folders](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/default_folder_hierarchy_membership_summary_tracks_top_level_ipm_folders.md)
- [hierarchy_transfer_without_eid_omits_folder_id_but_keeps_parent_folder_id](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_without_eid_omits_folder_id_but_keeps_parent_folder_id.md)
- [hierarchy_transfer_includes_folder_id_with_eid_extra_flag](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_includes_folder_id_with_eid_extra_flag.md)
- [hierarchy_transfer_calendar_includes_account_scoped_entry_id](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_calendar_includes_account_scoped_entry_id.md)
- [hierarchy_transfer_inbox_includes_calendar_identification_entry_id](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_inbox_includes_calendar_identification_entry_id.md)
- [hierarchy_transfer_respects_entry_id_exclusion](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_respects_entry_id_exclusion.md)
- [hierarchy_transfer_respects_default_post_message_class_exclusion](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_respects_default_post_message_class_exclusion.md)
- [hierarchy_transfer_respects_default_post_message_class_string8_exclusion](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_respects_default_post_message_class_string8_exclusion.md)
- [hierarchy_transfer_omits_custom_sync_root_and_projects_children](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_transfer_omits_custom_sync_root_and_projects_children.md)
- [content_sync_manifest_includes_special_folder_message_objects](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_sync_manifest_includes_special_folder_message_objects.md)
- [microsoft_oxcfxics_content_sync_progress_markers_follow_progress_flag_example](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_content_sync_progress_markers_follow_progress_flag_example.md)
- [content_sync_manifest_starts_fai_message_before_item_properties](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_sync_manifest_starts_fai_message_before_item_properties.md)
- [fai_foreign_source_key_identity_is_used_by_selected_and_full_idset_given](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/fai_foreign_source_key_identity_is_used_by_selected_and_full_idset_given.md)
- [normal_message_no_foreign_identifiers_uses_local_source_key_for_selection](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/normal_message_no_foreign_identifiers_uses_local_source_key_for_selection.md)
- [microsoft_oxcfxics_fai_content_sync_delimits_empty_child_collections_before_next_marker](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/microsoft_oxcfxics_fai_content_sync_delimits_empty_child_collections_before_next_marker.md)
- [content_sync_manifest_unicode_fai_uses_unicode_subject_and_fai_message_flag](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_sync_manifest_unicode_fai_uses_unicode_subject_and_fai_message_flag.md)
- [content_sync_manifest_applies_property_excludes_to_special_objects](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_sync_manifest_applies_property_excludes_to_special_objects.md)
- [content_sync_manifest_applies_string8_property_excludes_to_special_objects](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_sync_manifest_applies_string8_property_excludes_to_special_objects.md)
- [content_sync_manifest_applies_string8_property_includes_to_special_objects](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_sync_manifest_applies_string8_property_includes_to_special_objects.md)
- [content_sync_manifest_respects_normal_and_fai_scope_flags](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_sync_manifest_respects_normal_and_fai_scope_flags.md)
- [contains_bytes](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/contains_bytes.md)
- [hierarchy_sync_omits_content_activity_count_properties](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_sync_omits_content_activity_count_properties.md)
- [hierarchy_sync_excluded_properties_are_not_reintroduced_as_stable_folder_facts](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_sync_excluded_properties_are_not_reintroduced_as_stable_folder_facts.md)
- [final_sync_state_separates_object_idset_from_change_cnset](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/final_sync_state_separates_object_idset_from_change_cnset.md)
- [scoped_final_sync_state_uses_the_durable_inbox_counter](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/scoped_final_sync_state_uses_the_durable_inbox_counter.md)
- [replguid_globset_parser_decodes_push_singleton_client_state](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/replguid_globset_parser_decodes_push_singleton_client_state.md)
- [replguid_globset_parser_decodes_common_stack_range_and_bitmask](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/replguid_globset_parser_decodes_common_stack_range_and_bitmask.md)
- [hierarchy_and_content_cnsets_replay_in_globcnt_order_without_read_state_changes](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/hierarchy_and_content_cnsets_replay_in_globcnt_order_without_read_state_changes.md)
- [content_sync_state_keeps_normal_and_fai_cnsets_separate](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/content_sync_state_keeps_normal_and_fai_cnsets_separate.md)
- [special_message_headers_and_final_cnsets_share_durable_change_numbers](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/special_message_headers_and_final_cnsets_share_durable_change_numbers.md)
- [deleted_idset_uses_replid_globset_ranges](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/deleted_idset_uses_replid_globset_ranges.md)
- [assert_variable_property](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/assert_variable_property.md)
- [assert_variable_property_present](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/assert_variable_property_present.md)
- [assert_i32_property](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/assert_i32_property.md)
- [assert_i64_property](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/assert_i64_property.md)
- [assert_absent_property](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/assert_absent_property.md)
- [assert_bool_property](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/assert_bool_property.md)
- [assert_change_number_property](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/assert_change_number_property.md)
- [assert_tag_order](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/assert_tag_order.md)
- [assert_tag_sequence](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/assert_tag_sequence.md)
- [utf16z](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/utf16z.md)
- [test_email](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/tests/test_email.md)

# Imports

- `super::*`
- `lpe_storage::{JmapEmailAddress, JmapEmailMailboxState}`

# Member of

- [lpe-exchange](../../../../../packages/crates/lpe-exchange.md)
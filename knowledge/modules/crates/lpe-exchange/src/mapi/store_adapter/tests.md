---
type: Rust Module
title: tests
resource: crates/lpe-exchange/src/mapi/store_adapter/tests.rs#L1-L1483
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/super-super-sync-drafts-folder-id
  - external/super
  - external/std-collections-hashmap-vecdeque
  - external/std-time-systemtime
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [empty_session](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/empty_session.md)
- [single_rop_buffer](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/single_rop_buffer.md)
- [rop_buffer](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/rop_buffer.md)
- [deduplicate_mapi_identity_requests_keeps_distinct_kinds](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/deduplicate_mapi_identity_requests_keeps_distinct_kinds.md)
- [release_handle_zero_rop_buffer](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/release_handle_zero_rop_buffer.md)
- [mailbox](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/mailbox.md)
- [merge_requested_mailboxes_adds_custom_identity_rows](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/merge_requested_mailboxes_adds_custom_identity_rows.md)
- [search_folder_role_summary_includes_builtin_flags](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/search_folder_role_summary_includes_builtin_flags.md)
- [access_plan_includes_long_term_id_source_in_trailing_replid_form](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_includes_long_term_id_source_in_trailing_replid_form.md)
- [access_plan_resolves_learned_special_folder_aliases](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_resolves_learned_special_folder_aliases.md)
- [access_plan_does_not_decode_get_properties_payload_as_object_id](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_does_not_decode_get_properties_payload_as_object_id.md)
- [access_plan_loads_common_views_associated_contents_on_table_open](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_loads_common_views_associated_contents_on_table_open.md)
- [access_plan_cached_mode_transfer_state_get_buffer_uses_session_state](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_cached_mode_transfer_state_get_buffer_uses_session_state.md)
- [access_plan_hierarchy_query_ignores_unrelated_live_calendar_handle](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_hierarchy_query_ignores_unrelated_live_calendar_handle.md)
- [access_plan_root_depth_hierarchy_query_requires_snapshot_backed_contents](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_root_depth_hierarchy_query_requires_snapshot_backed_contents.md)
- [access_plan_hierarchy_seek_query_ignores_unrelated_live_calendar_handle](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_hierarchy_seek_query_ignores_unrelated_live_calendar_handle.md)
- [access_plan_contents_seek_from_end_still_requires_full_snapshot](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_contents_seek_from_end_still_requires_full_snapshot.md)
- [access_plan_normal_mail_contents_seek_uses_content_window_total](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_normal_mail_contents_seek_uses_content_window_total.md)
- [access_plan_normal_mail_contents_setcolumns_prefetches_first_row](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_normal_mail_contents_setcolumns_prefetches_first_row.md)
- [access_plan_non_mail_contents_query_rows_requires_full_snapshot](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_non_mail_contents_query_rows_requires_full_snapshot.md)
- [access_plan_associated_contents_query_rows_stays_store_selective](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_associated_contents_query_rows_stays_store_selective.md)
- [access_plan_common_views_query_rows_requests_common_views_backing_data](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_common_views_query_rows_requests_common_views_backing_data.md)
- [common_views_object_id_requires_snapshot_backed_contents](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/common_views_object_id_requires_snapshot_backed_contents.md)
- [default_contacts_object_id_requires_snapshot_backed_contents](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/default_contacts_object_id_requires_snapshot_backed_contents.md)
- [special_content_folder_ids_require_snapshot_backed_contents](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/special_content_folder_ids_require_snapshot_backed_contents.md)
- [access_plan_does_not_apply_mail_default_sort_to_contacts_contents_table](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_does_not_apply_mail_default_sort_to_contacts_contents_table.md)
- [access_plan_query_position_does_not_window_contacts_contents_table](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_query_position_does_not_window_contacts_contents_table.md)
- [access_plan_merges_seek_total_query_with_following_query_rows_window](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_merges_seek_total_query_with_following_query_rows_window.md)
- [access_plan_merges_overlapping_content_windows](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_merges_overlapping_content_windows.md)
- [access_plan_merges_content_window_that_bridges_existing_ranges](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_merges_content_window_that_bridges_existing_ranges.md)
- [access_plan_merges_total_probe_inside_existing_content_window](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_merges_total_probe_inside_existing_content_window.md)
- [access_plan_merges_existing_total_probe_inside_later_content_window](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_merges_existing_total_probe_inside_later_content_window.md)
- [access_plan_merges_total_probe_before_existing_content_window_without_widening](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_merges_total_probe_before_existing_content_window_without_widening.md)
- [access_plan_merges_existing_total_probe_before_later_content_window_without_widening](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_merges_existing_total_probe_before_later_content_window_without_widening.md)
- [access_plan_merges_total_probes_at_different_offsets](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_merges_total_probes_at_different_offsets.md)
- [access_plan_non_mail_contents_seek_still_requires_full_snapshot](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_non_mail_contents_seek_still_requires_full_snapshot.md)
- [access_plan_associated_contents_seek_stays_selective](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_associated_contents_seek_stays_selective.md)
- [access_plan_associated_contents_find_row_stays_selective](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_associated_contents_find_row_stays_selective.md)
- [access_plan_normal_contents_find_row_still_requires_full_snapshot](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_normal_contents_find_row_still_requires_full_snapshot.md)
- [access_plan_does_not_fetch_virtual_default_conversation_action_identity](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_does_not_fetch_virtual_default_conversation_action_identity.md)
- [access_plan_does_not_fetch_default_common_views_shortcut_identity](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_does_not_fetch_default_common_views_shortcut_identity.md)
- [access_plan_fetches_common_views_named_view_identity](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_fetches_common_views_named_view_identity.md)
- [access_plan_fetches_folder_named_view_identity](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_fetches_folder_named_view_identity.md)
- [access_plan_does_not_fetch_virtual_inbox_associated_config_identity](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_does_not_fetch_virtual_inbox_associated_config_identity.md)
- [access_plan_fetches_non_virtual_quick_step_associated_config_identity](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_fetches_non_virtual_quick_step_associated_config_identity.md)
- [access_plan_fetches_unbacked_contact_associated_config_identity](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_fetches_unbacked_contact_associated_config_identity.md)
- [access_plan_does_not_decode_set_properties_payload_as_import_source_key](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_does_not_decode_set_properties_payload_as_import_source_key.md)
- [access_plan_does_not_decode_set_properties_payload_as_read_state_change](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_does_not_decode_set_properties_payload_as_read_state_change.md)
- [access_plan_decodes_synchronization_import_read_state_changes](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_decodes_synchronization_import_read_state_changes.md)
- [access_plan_preloads_long_term_id_from_id_source](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_preloads_long_term_id_from_id_source.md)
- [missing_mapi_identity_summary_names_object_and_canonical_ids](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/missing_mapi_identity_summary_names_object_and_canonical_ids.md)
- [requested_store_identity_requires_backing_row_for_optional_mapi_state](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/requested_store_identity_requires_backing_row_for_optional_mapi_state.md)
- [unresolved_mapi_identity_summary_classifies_expected_special_and_invalid_ids](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/unresolved_mapi_identity_summary_classifies_expected_special_and_invalid_ids.md)
- [expected_unbacked_mapi_objects_exclude_non_virtual_quick_step_config](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/expected_unbacked_mapi_objects_exclude_non_virtual_quick_step_config.md)

# Imports

- `super::super::sync::DRAFTS_FOLDER_ID`
- `super::*`
- `std::collections::{HashMap, VecDeque}`
- `std::time::SystemTime`

# Member of

- [lpe-exchange](../../../../../../packages/crates/lpe-exchange.md)
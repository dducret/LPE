---
type: Rust Function
title: empty_session
resource: crates/lpe-exchange/src/mapi/store_adapter/tests.rs#L6-L57
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_includes_long_term_id_source_in_trailing_replid_form
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_resolves_learned_special_folder_aliases
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_does_not_decode_get_properties_payload_as_object_id
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_loads_common_views_associated_contents_on_table_open
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_cached_mode_transfer_state_get_buffer_uses_session_state
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_hierarchy_query_ignores_unrelated_live_calendar_handle
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_root_depth_hierarchy_query_requires_snapshot_backed_contents
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_hierarchy_seek_query_ignores_unrelated_live_calendar_handle
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_contents_seek_from_end_still_requires_full_snapshot
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_normal_mail_contents_seek_uses_content_window_total
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_normal_mail_contents_setcolumns_prefetches_first_row
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_non_mail_contents_query_rows_requires_full_snapshot
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_associated_contents_query_rows_stays_store_selective
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_common_views_query_rows_requests_common_views_backing_data
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_does_not_apply_mail_default_sort_to_contacts_contents_table
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_query_position_does_not_window_contacts_contents_table
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_non_mail_contents_seek_still_requires_full_snapshot
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_associated_contents_seek_stays_selective
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_associated_contents_find_row_stays_selective
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_normal_contents_find_row_still_requires_full_snapshot
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_does_not_fetch_virtual_default_conversation_action_identity
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_does_not_fetch_default_common_views_shortcut_identity
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_fetches_common_views_named_view_identity
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_fetches_folder_named_view_identity
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_does_not_fetch_virtual_inbox_associated_config_identity
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_fetches_non_virtual_quick_step_associated_config_identity
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_fetches_unbacked_contact_associated_config_identity
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_does_not_decode_set_properties_payload_as_import_source_key
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_does_not_decode_set_properties_payload_as_read_state_change
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_decodes_synchronization_import_read_state_changes
  - functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_preloads_long_term_id_from_id_source
---

# Signature

`fn empty_session() -> MapiSession`

# Called by

- [access_plan_includes_long_term_id_source_in_trailing_replid_form](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_includes_long_term_id_source_in_trailing_replid_form.md)
- [access_plan_resolves_learned_special_folder_aliases](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_resolves_learned_special_folder_aliases.md)
- [access_plan_does_not_decode_get_properties_payload_as_object_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_does_not_decode_get_properties_payload_as_object_id.md)
- [access_plan_loads_common_views_associated_contents_on_table_open](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_loads_common_views_associated_contents_on_table_open.md)
- [access_plan_cached_mode_transfer_state_get_buffer_uses_session_state](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_cached_mode_transfer_state_get_buffer_uses_session_state.md)
- [access_plan_hierarchy_query_ignores_unrelated_live_calendar_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_hierarchy_query_ignores_unrelated_live_calendar_handle.md)
- [access_plan_root_depth_hierarchy_query_requires_snapshot_backed_contents](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_root_depth_hierarchy_query_requires_snapshot_backed_contents.md)
- [access_plan_hierarchy_seek_query_ignores_unrelated_live_calendar_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_hierarchy_seek_query_ignores_unrelated_live_calendar_handle.md)
- [access_plan_contents_seek_from_end_still_requires_full_snapshot](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_contents_seek_from_end_still_requires_full_snapshot.md)
- [access_plan_normal_mail_contents_seek_uses_content_window_total](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_normal_mail_contents_seek_uses_content_window_total.md)
- [access_plan_normal_mail_contents_setcolumns_prefetches_first_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_normal_mail_contents_setcolumns_prefetches_first_row.md)
- [access_plan_non_mail_contents_query_rows_requires_full_snapshot](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_non_mail_contents_query_rows_requires_full_snapshot.md)
- [access_plan_associated_contents_query_rows_stays_store_selective](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_associated_contents_query_rows_stays_store_selective.md)
- [access_plan_common_views_query_rows_requests_common_views_backing_data](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_common_views_query_rows_requests_common_views_backing_data.md)
- [access_plan_does_not_apply_mail_default_sort_to_contacts_contents_table](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_does_not_apply_mail_default_sort_to_contacts_contents_table.md)
- [access_plan_query_position_does_not_window_contacts_contents_table](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_query_position_does_not_window_contacts_contents_table.md)
- [access_plan_non_mail_contents_seek_still_requires_full_snapshot](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_non_mail_contents_seek_still_requires_full_snapshot.md)
- [access_plan_associated_contents_seek_stays_selective](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_associated_contents_seek_stays_selective.md)
- [access_plan_associated_contents_find_row_stays_selective](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_associated_contents_find_row_stays_selective.md)
- [access_plan_normal_contents_find_row_still_requires_full_snapshot](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_normal_contents_find_row_still_requires_full_snapshot.md)
- [access_plan_does_not_fetch_virtual_default_conversation_action_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_does_not_fetch_virtual_default_conversation_action_identity.md)
- [access_plan_does_not_fetch_default_common_views_shortcut_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_does_not_fetch_default_common_views_shortcut_identity.md)
- [access_plan_fetches_common_views_named_view_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_fetches_common_views_named_view_identity.md)
- [access_plan_fetches_folder_named_view_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_fetches_folder_named_view_identity.md)
- [access_plan_does_not_fetch_virtual_inbox_associated_config_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_does_not_fetch_virtual_inbox_associated_config_identity.md)
- [access_plan_fetches_non_virtual_quick_step_associated_config_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_fetches_non_virtual_quick_step_associated_config_identity.md)
- [access_plan_fetches_unbacked_contact_associated_config_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_fetches_unbacked_contact_associated_config_identity.md)
- [access_plan_does_not_decode_set_properties_payload_as_import_source_key](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_does_not_decode_set_properties_payload_as_import_source_key.md)
- [access_plan_does_not_decode_set_properties_payload_as_read_state_change](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_does_not_decode_set_properties_payload_as_read_state_change.md)
- [access_plan_decodes_synchronization_import_read_state_changes](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_decodes_synchronization_import_read_state_changes.md)
- [access_plan_preloads_long_term_id_from_id_source](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/tests/access_plan_preloads_long_term_id_from_id_source.md)
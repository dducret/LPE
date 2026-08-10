---
type: Rust Function
title: test_mapi_session
resource: crates/lpe-exchange/src/mapi/dispatch/tests.rs#L3057-L3109
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/debug_named_property_context_reports_session_and_unresolved_properties
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/contents_table_named_property_context_reports_selected_columns
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/outlook_view_descriptor_named_property_context_reports_calendar_lids
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/calendar_contract_fingerprint_covers_client_normal_view_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/calendar_contract_fingerprint_covers_exact_selected_table_state
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/calendar_contract_fingerprint_bounds_large_named_property_registry
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/outlook_view_descriptor_named_property_context_is_empty_without_persisted_view
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/smart_input_variant_resets_inbox_fai_cursor_before_query_rows
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/default_view_advertisement_state_marks_matching_open
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/default_view_advertisement_preserves_matching_open_state
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/default_view_match_state_reports_pre_advertised_folder_open
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/default_view_advertisement_state_tracks_multiple_owner_folders
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/table_columns_normalize_stale_sharing_named_property_alias
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/table_columns_normalize_stale_sharing_alias_without_cached_mapping
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/table_columns_normalize_well_known_contact_email_named_property_alias
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/table_columns_normalize_outlook_contact_view_email_alias
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/table_columns_normalize_outlook_visible_inbox_view_property
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/table_columns_normalize_outlook_calendar_common_aliases
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/calendar_named_property_mapping_keeps_registered_database_ids
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/get_property_ids_from_names_returns_registered_well_known_property_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/get_property_ids_from_names_returns_registered_contact_source_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/get_property_ids_from_names_keeps_registered_reserved_range_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/store_named_property_mapping_rejects_session_collision
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/save_changes_success_response_preserves_containing_folder_handle_slot
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/save_changes_associated_message_restores_containing_folder_response_handle_slot
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/save_changes_navigation_shortcut_restores_common_views_folder_response_handle_slot
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/unadvertised_default_conversation_action_set_properties_is_rejected
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/virtual_default_conversation_action_set_rejects_wrong_folder
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/unadvertised_default_conversation_action_delete_properties_is_rejected
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_preserves_pending_table_notification_after_releasing_its_table
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_overflow_restores_deliverable_notification_batch
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_overflow_does_not_restore_unmatched_notification
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/release_only_execute_with_notification_target_requires_identity_scope
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/inbox_folder_type_getprops_probe_loads_store_snapshot
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/inbox_display_name_getprops_probe_loads_store_snapshot
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/root_folder_type_getprops_probe_stays_store_independent
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/root_default_folder_entry_id_getprops_probe_loads_store_snapshot
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/role_backed_special_folder_getprops_probes_load_store_snapshot
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/special_folder_getprops_probe_rejects_custom_properties
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/deleted_advertised_quick_step_create_can_reuse_existing_real_folder
---

# Signature

`fn test_mapi_session() -> MapiSession`

# Called by

- [debug_named_property_context_reports_session_and_unresolved_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/debug_named_property_context_reports_session_and_unresolved_properties.md)
- [contents_table_named_property_context_reports_selected_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/contents_table_named_property_context_reports_selected_columns.md)
- [outlook_view_descriptor_named_property_context_reports_calendar_lids](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/outlook_view_descriptor_named_property_context_reports_calendar_lids.md)
- [calendar_contract_fingerprint_covers_client_normal_view_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/calendar_contract_fingerprint_covers_client_normal_view_contract.md)
- [calendar_contract_fingerprint_covers_exact_selected_table_state](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/calendar_contract_fingerprint_covers_exact_selected_table_state.md)
- [calendar_contract_fingerprint_bounds_large_named_property_registry](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/calendar_contract_fingerprint_bounds_large_named_property_registry.md)
- [outlook_view_descriptor_named_property_context_is_empty_without_persisted_view](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/outlook_view_descriptor_named_property_context_is_empty_without_persisted_view.md)
- [smart_input_variant_resets_inbox_fai_cursor_before_query_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/smart_input_variant_resets_inbox_fai_cursor_before_query_rows.md)
- [default_view_advertisement_state_marks_matching_open](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/default_view_advertisement_state_marks_matching_open.md)
- [default_view_advertisement_preserves_matching_open_state](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/default_view_advertisement_preserves_matching_open_state.md)
- [default_view_match_state_reports_pre_advertised_folder_open](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/default_view_match_state_reports_pre_advertised_folder_open.md)
- [default_view_advertisement_state_tracks_multiple_owner_folders](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/default_view_advertisement_state_tracks_multiple_owner_folders.md)
- [table_columns_normalize_stale_sharing_named_property_alias](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/table_columns_normalize_stale_sharing_named_property_alias.md)
- [table_columns_normalize_stale_sharing_alias_without_cached_mapping](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/table_columns_normalize_stale_sharing_alias_without_cached_mapping.md)
- [table_columns_normalize_well_known_contact_email_named_property_alias](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/table_columns_normalize_well_known_contact_email_named_property_alias.md)
- [table_columns_normalize_outlook_contact_view_email_alias](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/table_columns_normalize_outlook_contact_view_email_alias.md)
- [table_columns_normalize_outlook_visible_inbox_view_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/table_columns_normalize_outlook_visible_inbox_view_property.md)
- [table_columns_normalize_outlook_calendar_common_aliases](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/table_columns_normalize_outlook_calendar_common_aliases.md)
- [calendar_named_property_mapping_keeps_registered_database_ids](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/calendar_named_property_mapping_keeps_registered_database_ids.md)
- [get_property_ids_from_names_returns_registered_well_known_property_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/get_property_ids_from_names_returns_registered_well_known_property_id.md)
- [get_property_ids_from_names_returns_registered_contact_source_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/get_property_ids_from_names_returns_registered_contact_source_id.md)
- [get_property_ids_from_names_keeps_registered_reserved_range_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/get_property_ids_from_names_keeps_registered_reserved_range_id.md)
- [store_named_property_mapping_rejects_session_collision](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/store_named_property_mapping_rejects_session_collision.md)
- [save_changes_success_response_preserves_containing_folder_handle_slot](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/save_changes_success_response_preserves_containing_folder_handle_slot.md)
- [save_changes_associated_message_restores_containing_folder_response_handle_slot](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/save_changes_associated_message_restores_containing_folder_response_handle_slot.md)
- [save_changes_navigation_shortcut_restores_common_views_folder_response_handle_slot](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/save_changes_navigation_shortcut_restores_common_views_folder_response_handle_slot.md)
- [unadvertised_default_conversation_action_set_properties_is_rejected](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/unadvertised_default_conversation_action_set_properties_is_rejected.md)
- [virtual_default_conversation_action_set_rejects_wrong_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/virtual_default_conversation_action_set_rejects_wrong_folder.md)
- [unadvertised_default_conversation_action_delete_properties_is_rejected](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/unadvertised_default_conversation_action_delete_properties_is_rejected.md)
- [execute_preserves_pending_table_notification_after_releasing_its_table](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_preserves_pending_table_notification_after_releasing_its_table.md)
- [execute_overflow_restores_deliverable_notification_batch](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_overflow_restores_deliverable_notification_batch.md)
- [execute_overflow_does_not_restore_unmatched_notification](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_overflow_does_not_restore_unmatched_notification.md)
- [release_only_execute_with_notification_target_requires_identity_scope](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/release_only_execute_with_notification_target_requires_identity_scope.md)
- [inbox_folder_type_getprops_probe_loads_store_snapshot](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/inbox_folder_type_getprops_probe_loads_store_snapshot.md)
- [inbox_display_name_getprops_probe_loads_store_snapshot](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/inbox_display_name_getprops_probe_loads_store_snapshot.md)
- [root_folder_type_getprops_probe_stays_store_independent](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/root_folder_type_getprops_probe_stays_store_independent.md)
- [root_default_folder_entry_id_getprops_probe_loads_store_snapshot](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/root_default_folder_entry_id_getprops_probe_loads_store_snapshot.md)
- [role_backed_special_folder_getprops_probes_load_store_snapshot](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/role_backed_special_folder_getprops_probes_load_store_snapshot.md)
- [special_folder_getprops_probe_rejects_custom_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/special_folder_getprops_probe_rejects_custom_properties.md)
- [deleted_advertised_quick_step_create_can_reuse_existing_real_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/deleted_advertised_quick_step_create_can_reuse_existing_real_folder.md)
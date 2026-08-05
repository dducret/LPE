---
type: Rust Method
title: with_associated_configs
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L339-L360
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/is_empty_synthetic_inbox_associated_config
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/is_stale_outlook_umolk_user_options_placeholder
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/apply_associated_config_identities
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/freebusy_open_prefers_delegate_message_over_stale_associated_config_identity
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/conversation_action_open_prefers_action_over_stale_associated_config_identity
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/associated_config_mutation_prefers_cumulative_saved_handle_over_stale_snapshot
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/inbox_associated_config_summary_reports_modeled_startup_rows
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/ipm_configuration_contract_summary_reports_required_columns_and_streams
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/associated_config_wire_summary_uses_requested_position
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/associated_config_debug_summaries_honor_table_restriction
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/folder_properties_for_open_reports_inbox_associated_content_count
  - functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan
  - functions/crates/lpe-exchange/src/mapi/sync/tests/inbox_fai_fasttransfer_boundaries_export_only_persisted_fai
  - functions/crates/lpe-exchange/src/mapi/sync/tests/empty_persisted_inbox_named_view_is_exported_by_fai_sync
  - functions/crates/lpe-exchange/src/mapi/sync/tests/calendar_fai_content_sync_preserves_imported_ics_identity_properties
  - functions/crates/lpe-exchange/src/mapi/sync/tests/outlook_inbox_fai_ics_omits_unsupported_message_identity_properties
  - functions/crates/lpe-exchange/src/mapi/sync/tests/associated_config_fai_content_sync_emits_valid_property_definitions
  - functions/crates/lpe-exchange/src/mapi/sync/tests/associated_config_fai_no_foreign_identifiers_uses_local_source_key
  - functions/crates/lpe-exchange/src/mapi/sync/tests/inbox_associated_content_sync_payload_emits_required_fai_properties
  - functions/crates/lpe-exchange/src/mapi/sync/tests/special_message_general_properties_follow_fast_transfer_property_filters
  - functions/crates/lpe-exchange/src/mapi/tables/tests/contacts_associated_find_row_preserves_table_position_for_contact_link_timestamp
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_find_row_followup_uses_the_original_rowset
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_content_restriction_projects_persisted_configs
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_query_rows_suppresses_duplicate_persisted_compact_named_view
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_query_rows_preserves_empty_persisted_compact_named_view
  - functions/crates/lpe-exchange/src/mapi/tables/tests/captured_calendar_fai_terminal_window_contains_only_canonical_configs
  - functions/crates/lpe-exchange/src/mapi/tables/tests/calendar_associated_query_rows_prefix_configuration_returns_calendar_config
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_query_rows_prefix_configuration_exposes_message_list_settings
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_sort_snapshot
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_extended_rule_snapshot
  - functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store
  - functions/crates/lpe-exchange/src/mapi_store/tests/inbox_associated_configs_do_not_emit_unpersisted_defaults
  - functions/crates/lpe-exchange/src/mapi_store/tests/empty_persisted_inbox_compact_named_view_remains_canonical
  - functions/crates/lpe-exchange/src/mapi_store/tests/empty_persisted_umolk_placeholder_does_not_shadow_exact_modeled_row
  - functions/crates/lpe-exchange/src/mapi_store/tests/stale_persisted_umolk_xml_placeholder_does_not_shadow_exact_modeled_row
  - functions/crates/lpe-exchange/src/mapi_store/tests/associated_config_sync_messages_use_persisted_rows_before_narrow_defaults
  - functions/crates/lpe-exchange/src/mapi_store/tests/associated_configs_keep_outlook_migration_markers_visible
  - functions/crates/lpe-exchange/src/mapi_store/tests/quick_step_settings_do_not_invent_custom_action_state
  - functions/crates/lpe-exchange/src/mapi_store/tests/contacts_project_exactly_the_persisted_contact_link_fai
  - functions/crates/lpe-exchange/src/mapi_store/tests/distinct_associated_fai_with_same_class_and_subject_survive_snapshot_projection
  - functions/crates/lpe-exchange/src/mapi_store/tests/modeled_virtual_associated_config_identity_opens_via_dynamic_id
---

# Signature

`pub(crate) fn with_associated_configs( mut self, configs: Vec<MapiAssociatedConfigRecord>, ) -> Self`

# Calls

- [is_empty_synthetic_inbox_associated_config](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_empty_synthetic_inbox_associated_config.md)
- [is_stale_outlook_umolk_user_options_placeholder](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_stale_outlook_umolk_user_options_placeholder.md)
- [apply_associated_config_identities](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/apply_associated_config_identities.md)

# Called by

- [freebusy_open_prefers_delegate_message_over_stale_associated_config_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/freebusy_open_prefers_delegate_message_over_stale_associated_config_identity.md)
- [conversation_action_open_prefers_action_over_stale_associated_config_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/conversation_action_open_prefers_action_over_stale_associated_config_identity.md)
- [associated_config_mutation_prefers_cumulative_saved_handle_over_stale_snapshot](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/associated_config_mutation_prefers_cumulative_saved_handle_over_stale_snapshot.md)
- [inbox_associated_config_summary_reports_modeled_startup_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/inbox_associated_config_summary_reports_modeled_startup_rows.md)
- [ipm_configuration_contract_summary_reports_required_columns_and_streams](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/ipm_configuration_contract_summary_reports_required_columns_and_streams.md)
- [associated_config_wire_summary_uses_requested_position](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/associated_config_wire_summary_uses_requested_position.md)
- [associated_config_debug_summaries_honor_table_restriction](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/associated_config_debug_summaries_honor_table_restriction.md)
- [folder_properties_for_open_reports_inbox_associated_content_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/folder_properties_for_open_reports_inbox_associated_content_count.md)
- [load_mapi_store_for_access_plan](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan.md)
- [inbox_fai_fasttransfer_boundaries_export_only_persisted_fai](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/inbox_fai_fasttransfer_boundaries_export_only_persisted_fai.md)
- [empty_persisted_inbox_named_view_is_exported_by_fai_sync](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/empty_persisted_inbox_named_view_is_exported_by_fai_sync.md)
- [calendar_fai_content_sync_preserves_imported_ics_identity_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/calendar_fai_content_sync_preserves_imported_ics_identity_properties.md)
- [outlook_inbox_fai_ics_omits_unsupported_message_identity_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/outlook_inbox_fai_ics_omits_unsupported_message_identity_properties.md)
- [associated_config_fai_content_sync_emits_valid_property_definitions](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/associated_config_fai_content_sync_emits_valid_property_definitions.md)
- [associated_config_fai_no_foreign_identifiers_uses_local_source_key](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/associated_config_fai_no_foreign_identifiers_uses_local_source_key.md)
- [inbox_associated_content_sync_payload_emits_required_fai_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/inbox_associated_content_sync_payload_emits_required_fai_properties.md)
- [special_message_general_properties_follow_fast_transfer_property_filters](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/special_message_general_properties_follow_fast_transfer_property_filters.md)
- [contacts_associated_find_row_preserves_table_position_for_contact_link_timestamp](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/contacts_associated_find_row_preserves_table_position_for_contact_link_timestamp.md)
- [inbox_associated_find_row_followup_uses_the_original_rowset](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_find_row_followup_uses_the_original_rowset.md)
- [inbox_associated_content_restriction_projects_persisted_configs](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_content_restriction_projects_persisted_configs.md)
- [inbox_associated_query_rows_suppresses_duplicate_persisted_compact_named_view](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_query_rows_suppresses_duplicate_persisted_compact_named_view.md)
- [inbox_associated_query_rows_preserves_empty_persisted_compact_named_view](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_query_rows_preserves_empty_persisted_compact_named_view.md)
- [captured_calendar_fai_terminal_window_contains_only_canonical_configs](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/captured_calendar_fai_terminal_window_contains_only_canonical_configs.md)
- [calendar_associated_query_rows_prefix_configuration_returns_calendar_config](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/calendar_associated_query_rows_prefix_configuration_returns_calendar_config.md)
- [inbox_associated_query_rows_prefix_configuration_exposes_message_list_settings](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_query_rows_prefix_configuration_exposes_message_list_settings.md)
- [inbox_associated_sort_snapshot](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_sort_snapshot.md)
- [inbox_associated_extended_rule_snapshot](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_extended_rule_snapshot.md)
- [load_mapi_mail_store](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store.md)
- [inbox_associated_configs_do_not_emit_unpersisted_defaults](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/inbox_associated_configs_do_not_emit_unpersisted_defaults.md)
- [empty_persisted_inbox_compact_named_view_remains_canonical](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/empty_persisted_inbox_compact_named_view_remains_canonical.md)
- [empty_persisted_umolk_placeholder_does_not_shadow_exact_modeled_row](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/empty_persisted_umolk_placeholder_does_not_shadow_exact_modeled_row.md)
- [stale_persisted_umolk_xml_placeholder_does_not_shadow_exact_modeled_row](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/stale_persisted_umolk_xml_placeholder_does_not_shadow_exact_modeled_row.md)
- [associated_config_sync_messages_use_persisted_rows_before_narrow_defaults](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/associated_config_sync_messages_use_persisted_rows_before_narrow_defaults.md)
- [associated_configs_keep_outlook_migration_markers_visible](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/associated_configs_keep_outlook_migration_markers_visible.md)
- [quick_step_settings_do_not_invent_custom_action_state](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/quick_step_settings_do_not_invent_custom_action_state.md)
- [contacts_project_exactly_the_persisted_contact_link_fai](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/contacts_project_exactly_the_persisted_contact_link_fai.md)
- [distinct_associated_fai_with_same_class_and_subject_survive_snapshot_projection](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/distinct_associated_fai_with_same_class_and_subject_survive_snapshot_projection.md)
- [modeled_virtual_associated_config_identity_opens_via_dynamic_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/modeled_virtual_associated_config_identity_opens_via_dynamic_id.md)
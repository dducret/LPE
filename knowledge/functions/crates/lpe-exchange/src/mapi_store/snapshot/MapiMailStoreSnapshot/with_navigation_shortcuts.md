---
type: Rust Method
title: with_navigation_shortcuts
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L237-L292
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/navigation_shortcut_message
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-exchange/src/mapi_store/format_navigation_shortcut_debug_summary
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/navigation_shortcut_messages
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/common_views_table_messages
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/inbox_associated_config_summary_reports_modeled_startup_rows
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/common_views_query_row_values_report_selected_wlink_columns
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/common_views_wlink_target_decoding_reports_inbox_match
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/common_views_wlink_contract_distinguishes_expected_link_defaults
  - functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan
  - functions/crates/lpe-exchange/src/mapi/sync/tests/common_views_fai_fasttransfer_boundaries_cover_only_persisted_shortcuts
  - functions/crates/lpe-exchange/src/mapi/sync/tests/common_views_shortcut_sync_uses_account_bound_entry_ids
  - functions/crates/lpe-exchange/src/mapi/sync/tests/common_views_shortcut_sync_does_not_emit_materialized_mail_header
  - functions/crates/lpe-exchange/src/mapi/sync/tests/common_views_group_header_sync_includes_group_identity_without_target
  - functions/crates/lpe-exchange/src/mapi/sync/tests/common_views_associated_content_sync_payload_emits_view_and_wunderbar_properties
  - functions/crates/lpe-exchange/src/mapi/sync/tests/navigation_shortcut_direct_copy_projects_its_account_scoped_entry_id
  - functions/crates/lpe-exchange/src/mapi/tables/tests/common_views_find_row_honors_restriction
  - functions/crates/lpe-exchange/src/mapi/tables/tests/common_views_find_row_matches_mail_wlink_folder_type
  - functions/crates/lpe-exchange/src/mapi/tables/tests/captured_common_views_query_rows_flags_heterogeneous_missing_columns
  - functions/crates/lpe-exchange/src/mapi/tables/tests/common_views_query_rows_projects_mailbox_store_object_entry_id
  - functions/crates/lpe-exchange/src/mapi/tables/tests/common_views_sort_snapshot
  - functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store
  - functions/crates/lpe-exchange/src/mapi_store/tests/common_views_uses_same_persisted_wlinks_and_durable_ids_for_table_and_ics
  - functions/crates/lpe-exchange/src/mapi_store/tests/common_views_preserves_persisted_calendar_group_and_shortcut_identity
  - functions/crates/lpe-exchange/src/mapi_store/tests/common_views_preserves_distinct_persisted_navigation_shortcuts_with_matching_properties
  - functions/crates/lpe-exchange/src/mapi_store/tests/common_views_does_not_materialize_mail_group_header_for_persisted_favorite_links
  - functions/crates/lpe-exchange/src/mapi_store/tests/common_views_projects_persisted_default_mail_favorites_in_startup_table
  - functions/crates/lpe-exchange/src/mapi_store/tests/common_views_projects_distinct_supported_module_shortcuts_in_startup_table
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_common_views_create_group_header_and_link_persists_and_reloads
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_microsoft_oxocfg_same_target_wlinks_round_trip_distinctly
---

# Signature

`pub(crate) fn with_navigation_shortcuts( mut self, navigation_shortcuts: Vec<MapiNavigationShortcutRecord>, ) -> Self`

# Calls

- [navigation_shortcut_message](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/navigation_shortcut_message.md)
- [expect](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [format_navigation_shortcut_debug_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/format_navigation_shortcut_debug_summary.md)
- [navigation_shortcut_messages](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/navigation_shortcut_messages.md)
- [common_views_table_messages](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/common_views_table_messages.md)

# Called by

- [inbox_associated_config_summary_reports_modeled_startup_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/inbox_associated_config_summary_reports_modeled_startup_rows.md)
- [common_views_query_row_values_report_selected_wlink_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/common_views_query_row_values_report_selected_wlink_columns.md)
- [common_views_wlink_target_decoding_reports_inbox_match](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/common_views_wlink_target_decoding_reports_inbox_match.md)
- [common_views_wlink_contract_distinguishes_expected_link_defaults](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/associated_config/common_views_wlink_contract_distinguishes_expected_link_defaults.md)
- [load_mapi_store_for_access_plan](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan.md)
- [common_views_fai_fasttransfer_boundaries_cover_only_persisted_shortcuts](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/common_views_fai_fasttransfer_boundaries_cover_only_persisted_shortcuts.md)
- [common_views_shortcut_sync_uses_account_bound_entry_ids](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/common_views_shortcut_sync_uses_account_bound_entry_ids.md)
- [common_views_shortcut_sync_does_not_emit_materialized_mail_header](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/common_views_shortcut_sync_does_not_emit_materialized_mail_header.md)
- [common_views_group_header_sync_includes_group_identity_without_target](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/common_views_group_header_sync_includes_group_identity_without_target.md)
- [common_views_associated_content_sync_payload_emits_view_and_wunderbar_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/common_views_associated_content_sync_payload_emits_view_and_wunderbar_properties.md)
- [navigation_shortcut_direct_copy_projects_its_account_scoped_entry_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/tests/navigation_shortcut_direct_copy_projects_its_account_scoped_entry_id.md)
- [common_views_find_row_honors_restriction](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/common_views_find_row_honors_restriction.md)
- [common_views_find_row_matches_mail_wlink_folder_type](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/common_views_find_row_matches_mail_wlink_folder_type.md)
- [captured_common_views_query_rows_flags_heterogeneous_missing_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/captured_common_views_query_rows_flags_heterogeneous_missing_columns.md)
- [common_views_query_rows_projects_mailbox_store_object_entry_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/common_views_query_rows_projects_mailbox_store_object_entry_id.md)
- [common_views_sort_snapshot](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/common_views_sort_snapshot.md)
- [load_mapi_mail_store](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/T/mapistore/load_mapi_mail_store.md)
- [common_views_uses_same_persisted_wlinks_and_durable_ids_for_table_and_ics](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/common_views_uses_same_persisted_wlinks_and_durable_ids_for_table_and_ics.md)
- [common_views_preserves_persisted_calendar_group_and_shortcut_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/common_views_preserves_persisted_calendar_group_and_shortcut_identity.md)
- [common_views_preserves_distinct_persisted_navigation_shortcuts_with_matching_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/common_views_preserves_distinct_persisted_navigation_shortcuts_with_matching_properties.md)
- [common_views_does_not_materialize_mail_group_header_for_persisted_favorite_links](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/common_views_does_not_materialize_mail_group_header_for_persisted_favorite_links.md)
- [common_views_projects_persisted_default_mail_favorites_in_startup_table](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/common_views_projects_persisted_default_mail_favorites_in_startup_table.md)
- [common_views_projects_distinct_supported_module_shortcuts_in_startup_table](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/common_views_projects_distinct_supported_module_shortcuts_in_startup_table.md)
- [mapi_over_http_common_views_create_group_header_and_link_persists_and_reloads](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_common_views_create_group_header_and_link_persists_and_reloads.md)
- [mapi_over_http_microsoft_oxocfg_same_target_wlinks_round_trip_distinctly](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_microsoft_oxocfg_same_target_wlinks_round_trip_distinctly.md)
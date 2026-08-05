---
type: Rust Method
title: common_views_table_messages
resource: crates/lpe-exchange/src/mapi_store/snapshot.rs#L1252-L1256
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/canonical_common_views_fai_messages
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/search_folder_definition_message_for_open
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_common_views_inbox_shortcut_context
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_common_views_wlink_target_decoding
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_common_views_wlink_contract_summary
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_common_views_query_row_window
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_outlook_query_row_values_inner
  - functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom
  - functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/counts/associated_folder_message_count
  - functions/crates/lpe-exchange/src/mapi/tables/counts/restricted_associated_folder_message_count
  - functions/crates/lpe-exchange/src/mapi/tables/diagnostics/outlook_bootstrap_row_invariant_summaries
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
  - functions/crates/lpe-exchange/src/mapi/tables/tests/common_views_wlink_query_rows_do_not_add_named_views_without_restriction
  - functions/crates/lpe-exchange/src/mapi/tables/tests/captured_common_views_query_rows_flags_heterogeneous_missing_columns
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_navigation_shortcuts
  - functions/crates/lpe-exchange/src/mapi_store/tests/empty_common_views_exposes_no_synthetic_fai
  - functions/crates/lpe-exchange/src/mapi_store/tests/common_views_uses_same_persisted_wlinks_and_durable_ids_for_table_and_ics
  - functions/crates/lpe-exchange/src/mapi_store/tests/common_views_preserves_persisted_calendar_group_and_shortcut_identity
  - functions/crates/lpe-exchange/src/mapi_store/tests/common_views_preserves_distinct_persisted_navigation_shortcuts_with_matching_properties
  - functions/crates/lpe-exchange/src/mapi_store/tests/common_views_projects_persisted_default_mail_favorites_in_startup_table
  - functions/crates/lpe-exchange/src/mapi_store/tests/common_views_projects_distinct_supported_module_shortcuts_in_startup_table
---

# Signature

`pub(crate) fn common_views_table_messages( &self, ) -> impl Iterator<Item = MapiCommonViewsMessage>`

# Calls

- [canonical_common_views_fai_messages](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/canonical_common_views_fai_messages.md)

# Called by

- [search_folder_definition_message_for_open](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/search_folder_definition_message_for_open.md)
- [format_common_views_inbox_shortcut_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_common_views_inbox_shortcut_context.md)
- [format_common_views_wlink_target_decoding](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_common_views_wlink_target_decoding.md)
- [format_common_views_wlink_contract_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/format_common_views_wlink_contract_summary.md)
- [format_common_views_query_row_window](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_common_views_query_row_window.md)
- [format_outlook_query_row_values_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_outlook_query_row_values_inner.md)
- [rop_get_properties_specific_response_with_custom](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom.md)
- [rop_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)
- [associated_folder_message_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/associated_folder_message_count.md)
- [restricted_associated_folder_message_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/restricted_associated_folder_message_count.md)
- [outlook_bootstrap_row_invariant_summaries](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/diagnostics/outlook_bootstrap_row_invariant_summaries.md)
- [rop_query_rows_response_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)
- [common_views_wlink_query_rows_do_not_add_named_views_without_restriction](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/common_views_wlink_query_rows_do_not_add_named_views_without_restriction.md)
- [captured_common_views_query_rows_flags_heterogeneous_missing_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/captured_common_views_query_rows_flags_heterogeneous_missing_columns.md)
- [with_navigation_shortcuts](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/with_navigation_shortcuts.md)
- [empty_common_views_exposes_no_synthetic_fai](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/empty_common_views_exposes_no_synthetic_fai.md)
- [common_views_uses_same_persisted_wlinks_and_durable_ids_for_table_and_ics](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/common_views_uses_same_persisted_wlinks_and_durable_ids_for_table_and_ics.md)
- [common_views_preserves_persisted_calendar_group_and_shortcut_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/common_views_preserves_persisted_calendar_group_and_shortcut_identity.md)
- [common_views_preserves_distinct_persisted_navigation_shortcuts_with_matching_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/common_views_preserves_distinct_persisted_navigation_shortcuts_with_matching_properties.md)
- [common_views_projects_persisted_default_mail_favorites_in_startup_table](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/common_views_projects_persisted_default_mail_favorites_in_startup_table.md)
- [common_views_projects_distinct_supported_module_shortcuts_in_startup_table](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/tests/common_views_projects_distinct_supported_module_shortcuts_in_startup_table.md)
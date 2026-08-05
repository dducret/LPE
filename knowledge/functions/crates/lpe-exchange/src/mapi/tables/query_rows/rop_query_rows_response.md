---
type: Rust Function
title: rop_query_rows_response
resource: crates/lpe-exchange/src/mapi/tables/query_rows.rs#L4-L21
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/tests/default_contacts_contents_table_uses_contact_rows_and_columns
  - functions/crates/lpe-exchange/src/mapi/tables/tests/quick_step_settings_normal_query_rows_returns_end_without_rows
  - functions/crates/lpe-exchange/src/mapi/tables/tests/sync_issues_query_rows_returns_no_children_until_backed
  - functions/crates/lpe-exchange/src/mapi/tables/tests/query_rows_request_validation_matches_microsoft_flags
  - functions/crates/lpe-exchange/src/mapi/tables/tests/query_rows_origin_tracks_cursor_boundary
  - functions/crates/lpe-exchange/src/mapi/tables/tests/query_rows_origin_uses_global_position_for_windowed_content_tables
  - functions/crates/lpe-exchange/src/mapi/tables/tests/query_rows_ignores_incomplete_windowed_content_table_rows
  - functions/crates/lpe-exchange/src/mapi/tables/tests/restricted_associated_query_position_reports_filtered_row_count
  - functions/crates/lpe-exchange/src/mapi/tables/tests/captured_calendar_table_query_rows_projects_exact_requested_property_row
  - functions/crates/lpe-exchange/src/mapi/tables/tests/query_rows_clamps_stale_cursor_to_current_row_count
  - functions/crates/lpe-exchange/src/mapi/tables/tests/microsoft_oxosrch_common_views_projects_search_folder_definition_messages
  - functions/crates/lpe-exchange/src/mapi/tables/tests/common_views_query_rows_uses_account_bound_wlink_entry_ids
  - functions/crates/lpe-exchange/src/mapi/tables/tests/common_views_wlink_query_rows_do_not_add_named_views_without_restriction
  - functions/crates/lpe-exchange/src/mapi/tables/tests/common_views_restricted_named_view_query_rows_are_empty_without_persisted_fai
  - functions/crates/lpe-exchange/src/mapi/tables/tests/common_views_query_rows_uses_wlink_sort_order
  - functions/crates/lpe-exchange/src/mapi/tables/tests/captured_common_views_query_rows_flags_heterogeneous_missing_columns
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_broad_configuration_find_row_projects_single_followup_row
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_find_row_followup_uses_the_original_rowset
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_query_rows_uses_sort_order
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_query_rows_includes_persisted_extended_rule_message
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_query_rows_suppresses_duplicate_persisted_compact_named_view
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_query_rows_preserves_empty_persisted_compact_named_view
  - functions/crates/lpe-exchange/src/mapi/tables/tests/junk_associated_query_rows_do_not_invent_default_named_view
  - functions/crates/lpe-exchange/src/mapi/tables/tests/contacts_associated_query_rows_do_not_invent_default_named_view_or_helpers
  - functions/crates/lpe-exchange/src/mapi/tables/tests/calendar_associated_query_rows_do_not_inject_synthetic_default_named_view
  - functions/crates/lpe-exchange/src/mapi/tables/tests/captured_calendar_fai_terminal_window_contains_only_canonical_configs
  - functions/crates/lpe-exchange/src/mapi/tables/tests/calendar_associated_query_rows_prefix_configuration_returns_calendar_config
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_query_rows_prefix_configuration_exposes_message_list_settings
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_query_rows_prefix_configuration_suppresses_virtual_elc
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_query_rows_uses_standard_property_rows_for_complete_rows
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_query_rows_does_not_create_virtual_umolk_user_options
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_query_rows_does_not_create_virtual_mrm_configuration
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_query_rows_returns_virtual_rule_organizer
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_query_rows_default_columns_cover_required_configuration_contract
  - functions/crates/lpe-exchange/src/mapi/tables/tests/normal_inbox_query_rows_projects_sender_and_delivery_time
  - functions/crates/lpe-exchange/src/mapi/tables/tests/microsoft_contents_table_query_find_and_expand_require_set_columns
---

# Signature

`pub(in crate::mapi) fn rop_query_rows_response( request: &RopRequest, object: Option<&mut MapiObject>, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, mailbox_guid: Uuid, ) -> Vec<u8>`

# Calls

- [rop_query_rows_response_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)

# Called by

- [default_contacts_contents_table_uses_contact_rows_and_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/default_contacts_contents_table_uses_contact_rows_and_columns.md)
- [quick_step_settings_normal_query_rows_returns_end_without_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/quick_step_settings_normal_query_rows_returns_end_without_rows.md)
- [sync_issues_query_rows_returns_no_children_until_backed](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/sync_issues_query_rows_returns_no_children_until_backed.md)
- [query_rows_request_validation_matches_microsoft_flags](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/query_rows_request_validation_matches_microsoft_flags.md)
- [query_rows_origin_tracks_cursor_boundary](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/query_rows_origin_tracks_cursor_boundary.md)
- [query_rows_origin_uses_global_position_for_windowed_content_tables](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/query_rows_origin_uses_global_position_for_windowed_content_tables.md)
- [query_rows_ignores_incomplete_windowed_content_table_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/query_rows_ignores_incomplete_windowed_content_table_rows.md)
- [restricted_associated_query_position_reports_filtered_row_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/restricted_associated_query_position_reports_filtered_row_count.md)
- [captured_calendar_table_query_rows_projects_exact_requested_property_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/captured_calendar_table_query_rows_projects_exact_requested_property_row.md)
- [query_rows_clamps_stale_cursor_to_current_row_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/query_rows_clamps_stale_cursor_to_current_row_count.md)
- [microsoft_oxosrch_common_views_projects_search_folder_definition_messages](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/microsoft_oxosrch_common_views_projects_search_folder_definition_messages.md)
- [common_views_query_rows_uses_account_bound_wlink_entry_ids](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/common_views_query_rows_uses_account_bound_wlink_entry_ids.md)
- [common_views_wlink_query_rows_do_not_add_named_views_without_restriction](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/common_views_wlink_query_rows_do_not_add_named_views_without_restriction.md)
- [common_views_restricted_named_view_query_rows_are_empty_without_persisted_fai](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/common_views_restricted_named_view_query_rows_are_empty_without_persisted_fai.md)
- [common_views_query_rows_uses_wlink_sort_order](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/common_views_query_rows_uses_wlink_sort_order.md)
- [captured_common_views_query_rows_flags_heterogeneous_missing_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/captured_common_views_query_rows_flags_heterogeneous_missing_columns.md)
- [inbox_associated_broad_configuration_find_row_projects_single_followup_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_broad_configuration_find_row_projects_single_followup_row.md)
- [inbox_associated_find_row_followup_uses_the_original_rowset](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_find_row_followup_uses_the_original_rowset.md)
- [inbox_associated_query_rows_uses_sort_order](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_query_rows_uses_sort_order.md)
- [inbox_associated_query_rows_includes_persisted_extended_rule_message](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_query_rows_includes_persisted_extended_rule_message.md)
- [inbox_associated_query_rows_suppresses_duplicate_persisted_compact_named_view](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_query_rows_suppresses_duplicate_persisted_compact_named_view.md)
- [inbox_associated_query_rows_preserves_empty_persisted_compact_named_view](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_query_rows_preserves_empty_persisted_compact_named_view.md)
- [junk_associated_query_rows_do_not_invent_default_named_view](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/junk_associated_query_rows_do_not_invent_default_named_view.md)
- [contacts_associated_query_rows_do_not_invent_default_named_view_or_helpers](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/contacts_associated_query_rows_do_not_invent_default_named_view_or_helpers.md)
- [calendar_associated_query_rows_do_not_inject_synthetic_default_named_view](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/calendar_associated_query_rows_do_not_inject_synthetic_default_named_view.md)
- [captured_calendar_fai_terminal_window_contains_only_canonical_configs](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/captured_calendar_fai_terminal_window_contains_only_canonical_configs.md)
- [calendar_associated_query_rows_prefix_configuration_returns_calendar_config](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/calendar_associated_query_rows_prefix_configuration_returns_calendar_config.md)
- [inbox_associated_query_rows_prefix_configuration_exposes_message_list_settings](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_query_rows_prefix_configuration_exposes_message_list_settings.md)
- [inbox_associated_query_rows_prefix_configuration_suppresses_virtual_elc](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_query_rows_prefix_configuration_suppresses_virtual_elc.md)
- [inbox_associated_query_rows_uses_standard_property_rows_for_complete_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_query_rows_uses_standard_property_rows_for_complete_rows.md)
- [inbox_associated_query_rows_does_not_create_virtual_umolk_user_options](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_query_rows_does_not_create_virtual_umolk_user_options.md)
- [inbox_associated_query_rows_does_not_create_virtual_mrm_configuration](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_query_rows_does_not_create_virtual_mrm_configuration.md)
- [inbox_associated_query_rows_returns_virtual_rule_organizer](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_query_rows_returns_virtual_rule_organizer.md)
- [inbox_associated_query_rows_default_columns_cover_required_configuration_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_query_rows_default_columns_cover_required_configuration_contract.md)
- [normal_inbox_query_rows_projects_sender_and_delivery_time](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/normal_inbox_query_rows_projects_sender_and_delivery_time.md)
- [microsoft_contents_table_query_find_and_expand_require_set_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/microsoft_contents_table_query_find_and_expand_require_set_columns.md)
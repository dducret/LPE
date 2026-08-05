---
type: Rust Function
title: assert_response_contains_utf16
resource: crates/lpe-exchange/src/mapi/tables/tests.rs#L8816-L8821
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/tests/default_contacts_contents_table_uses_contact_rows_and_columns
  - functions/crates/lpe-exchange/src/mapi/tables/tests/contact_table_projects_missing_secondary_email_slots_as_empty_strings
  - functions/crates/lpe-exchange/src/mapi/tables/tests/query_rows_ignores_incomplete_windowed_content_table_rows
  - functions/crates/lpe-exchange/src/mapi/tables/tests/find_row_uses_windowed_content_table_rows_with_global_position
  - functions/crates/lpe-exchange/src/mapi/tables/tests/find_row_beginning_origin_keeps_windowed_global_position
  - functions/crates/lpe-exchange/src/mapi/tables/tests/find_row_beginning_origin_falls_back_when_complete_rows_are_loaded
  - functions/crates/lpe-exchange/src/mapi/tables/tests/mapi_hierarchy_row_projects_inbox_display_name
  - functions/crates/lpe-exchange/src/mapi/tables/tests/calendar_contents_find_row_matches_outlook_date_window
  - functions/crates/lpe-exchange/src/mapi/tables/tests/common_views_find_row_matches_mail_wlink_folder_type
  - functions/crates/lpe-exchange/src/mapi/tables/tests/common_views_wlink_query_rows_do_not_add_named_views_without_restriction
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_find_row_returns_folder_local_default_named_view
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_find_row_returns_virtual_rule_organizer
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_find_row_returns_the_folder_local_default_for_a_broad_startup_lookup
  - functions/crates/lpe-exchange/src/mapi/tables/tests/contacts_associated_find_row_preserves_table_position_for_contact_link_timestamp
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_exact_configuration_find_row_uses_sort_order
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_broad_configuration_find_row_projects_single_followup_row
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_find_row_followup_uses_the_original_rowset
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_find_row_can_return_a_persisted_extended_rule_message
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_query_rows_returns_virtual_rule_organizer
---

# Signature

`fn assert_response_contains_utf16(response: &[u8], value: &str)`

# Called by

- [default_contacts_contents_table_uses_contact_rows_and_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/default_contacts_contents_table_uses_contact_rows_and_columns.md)
- [contact_table_projects_missing_secondary_email_slots_as_empty_strings](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/contact_table_projects_missing_secondary_email_slots_as_empty_strings.md)
- [query_rows_ignores_incomplete_windowed_content_table_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/query_rows_ignores_incomplete_windowed_content_table_rows.md)
- [find_row_uses_windowed_content_table_rows_with_global_position](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/find_row_uses_windowed_content_table_rows_with_global_position.md)
- [find_row_beginning_origin_keeps_windowed_global_position](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/find_row_beginning_origin_keeps_windowed_global_position.md)
- [find_row_beginning_origin_falls_back_when_complete_rows_are_loaded](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/find_row_beginning_origin_falls_back_when_complete_rows_are_loaded.md)
- [mapi_hierarchy_row_projects_inbox_display_name](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/mapi_hierarchy_row_projects_inbox_display_name.md)
- [calendar_contents_find_row_matches_outlook_date_window](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/calendar_contents_find_row_matches_outlook_date_window.md)
- [common_views_find_row_matches_mail_wlink_folder_type](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/common_views_find_row_matches_mail_wlink_folder_type.md)
- [common_views_wlink_query_rows_do_not_add_named_views_without_restriction](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/common_views_wlink_query_rows_do_not_add_named_views_without_restriction.md)
- [inbox_associated_find_row_returns_folder_local_default_named_view](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_find_row_returns_folder_local_default_named_view.md)
- [inbox_associated_find_row_returns_virtual_rule_organizer](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_find_row_returns_virtual_rule_organizer.md)
- [inbox_associated_find_row_returns_the_folder_local_default_for_a_broad_startup_lookup](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_find_row_returns_the_folder_local_default_for_a_broad_startup_lookup.md)
- [contacts_associated_find_row_preserves_table_position_for_contact_link_timestamp](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/contacts_associated_find_row_preserves_table_position_for_contact_link_timestamp.md)
- [inbox_associated_exact_configuration_find_row_uses_sort_order](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_exact_configuration_find_row_uses_sort_order.md)
- [inbox_associated_broad_configuration_find_row_projects_single_followup_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_broad_configuration_find_row_projects_single_followup_row.md)
- [inbox_associated_find_row_followup_uses_the_original_rowset](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_find_row_followup_uses_the_original_rowset.md)
- [inbox_associated_find_row_can_return_a_persisted_extended_rule_message](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_find_row_can_return_a_persisted_extended_rule_message.md)
- [inbox_associated_query_rows_returns_virtual_rule_organizer](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_query_rows_returns_virtual_rule_organizer.md)
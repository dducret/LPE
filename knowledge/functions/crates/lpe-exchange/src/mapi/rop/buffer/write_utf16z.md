---
type: Rust Function
title: write_utf16z
resource: crates/lpe-exchange/src/mapi/rop/buffer.rs#L124-L129
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/endpoint_url_response
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/write_address_book_property_value
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/write_nspi_multi_string
  - functions/crates/lpe-exchange/src/mapi/permissions/serialize_permission_row
  - functions/crates/lpe-exchange/src/mapi/properties/tests/push_content_restriction
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_multi_string
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_typed_string
  - functions/crates/lpe-exchange/src/mapi/rop/request_reader/read_rop_request_with_logon_id
  - functions/crates/lpe-exchange/src/mapi/rop/tests/modify_recipients_parses_outlook_flagged_recipient_property_row
  - functions/crates/lpe-exchange/src/mapi/rop/tests/modify_recipients_accepts_microsoft_message_example_columns
  - functions/crates/lpe-exchange/src/mapi/rop/tests/restriction_parser_preserves_content_fuzzy_levels
  - functions/crates/lpe-exchange/src/mapi/tables/attachments/serialize_attachment_row
  - functions/crates/lpe-exchange/src/mapi/tables/attachments/serialize_pending_attachment_row
  - functions/crates/lpe-exchange/src/mapi/tables/attachments/serialize_saved_attachment_row
  - functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_row_with_table_instance
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_advertised_special_folder_row_with_counts_and_change_number
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_root_folder_row
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_ipm_subtree_folder_row
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_folder_row_with_context
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_collaboration_folder_row_with_context
  - functions/crates/lpe-exchange/src/mapi/tables/public_folders/serialize_public_folder_row
  - functions/crates/lpe-exchange/src/mapi/tables/public_folders/serialize_public_folder_item_row
  - functions/crates/lpe-exchange/src/mapi/tables/recipients/serialize_recipient_row
  - functions/crates/lpe-exchange/src/mapi/tables/recoverable_items/serialize_recoverable_item_row
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default
  - functions/crates/lpe-exchange/src/mapi/tables/rules/serialize_rule_row
  - functions/crates/lpe-exchange/src/mapi/tables/tests/query_rows_truncates_variable_property_values_to_microsoft_limit
  - functions/crates/lpe-exchange/src/mapi/tables/tests/find_row_uses_windowed_content_table_rows_with_global_position
  - functions/crates/lpe-exchange/src/mapi/tables/tests/find_row_beginning_origin_keeps_windowed_global_position
  - functions/crates/lpe-exchange/src/mapi/tables/tests/find_row_beginning_origin_falls_back_when_complete_rows_are_loaded
  - functions/crates/lpe-exchange/src/mapi/tables/tests/captured_calendar_table_query_rows_projects_exact_requested_property_row
  - functions/crates/lpe-exchange/src/mapi/tables/tests/common_views_find_row_honors_restriction
  - functions/crates/lpe-exchange/src/mapi/tables/tests/contacts_contents_find_row_matches_display_name
  - functions/crates/lpe-exchange/src/mapi/tables/tests/common_views_find_row_does_not_invent_default_compact_named_view
  - functions/crates/lpe-exchange/src/mapi/tables/tests/common_views_find_row_does_not_invent_default_sent_to_named_view
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_exact_virtual_find_row_does_not_inject_a_row
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_find_row_returns_not_found_for_unstored_aggregation_config
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_find_row_does_not_invent_a_default_for_a_broad_startup_lookup
  - functions/crates/lpe-exchange/src/mapi/tables/tests/quick_step_associated_find_row_does_not_return_synthetic_custom_action
  - functions/crates/lpe-exchange/src/mapi/tables/tests/contacts_associated_find_row_preserves_table_position_for_contact_link_timestamp
  - functions/crates/lpe-exchange/src/mapi/tables/tests/empty_conversation_action_settings_find_row_returns_not_found
  - functions/crates/lpe-exchange/src/mapi/tables/tests/conversation_action_settings_find_row_honors_restriction
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_exact_configuration_find_row_uses_sort_order
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_broad_configuration_find_row_projects_single_followup_row
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_exact_named_view_find_row_respects_existing_table_restriction
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_find_row_followup_uses_the_original_rowset
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_find_row_can_return_a_persisted_extended_rule_message
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_find_row_response_for_message_class
  - functions/crates/lpe-exchange/src/mapi/tables/tests/contact_folder_associated_find_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/tests/utf16_position
  - functions/crates/lpe-exchange/src/mapi/tables/tests/utf16_occurrences
  - functions/crates/lpe-exchange/src/mapi/tables/tests/microsoft_contents_table_query_find_and_expand_require_set_columns
  - functions/crates/lpe-exchange/src/mapi/transport/connect_response
  - functions/crates/lpe-exchange/src/mapi/transport/tests/connect_body_debug_summary_decodes_fields
---

# Signature

`pub(in crate::mapi) fn write_utf16z(body: &mut Vec<u8>, value: &str)`

# Called by

- [endpoint_url_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/endpoint_url_response.md)
- [write_address_book_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/write_address_book_property_value.md)
- [write_nspi_multi_string](../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/write_nspi_multi_string.md)
- [serialize_permission_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/permissions/serialize_permission_row.md)
- [push_content_restriction](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/push_content_restriction.md)
- [write_mapi_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value.md)
- [write_multi_string](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_multi_string.md)
- [write_typed_string](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_typed_string.md)
- [read_rop_request_with_logon_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/request_reader/read_rop_request_with_logon_id.md)
- [modify_recipients_parses_outlook_flagged_recipient_property_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/modify_recipients_parses_outlook_flagged_recipient_property_row.md)
- [modify_recipients_accepts_microsoft_message_example_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/modify_recipients_accepts_microsoft_message_example_columns.md)
- [restriction_parser_preserves_content_fuzzy_levels](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/restriction_parser_preserves_content_fuzzy_levels.md)
- [serialize_attachment_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/attachments/serialize_attachment_row.md)
- [serialize_pending_attachment_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/attachments/serialize_pending_attachment_row.md)
- [serialize_saved_attachment_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/attachments/serialize_saved_attachment_row.md)
- [serialize_message_row_with_table_instance](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/contents/serialize_message_row_with_table_instance.md)
- [serialize_advertised_special_folder_row_with_counts_and_change_number](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_advertised_special_folder_row_with_counts_and_change_number.md)
- [serialize_root_folder_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_root_folder_row.md)
- [serialize_ipm_subtree_folder_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_ipm_subtree_folder_row.md)
- [serialize_folder_row_with_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_folder_row_with_context.md)
- [serialize_collaboration_folder_row_with_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_collaboration_folder_row_with_context.md)
- [serialize_public_folder_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/public_folders/serialize_public_folder_row.md)
- [serialize_public_folder_item_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/public_folders/serialize_public_folder_item_row.md)
- [serialize_recipient_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/recipients/serialize_recipient_row.md)
- [serialize_recoverable_item_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/recoverable_items/serialize_recoverable_item_row.md)
- [write_property_default](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default.md)
- [serialize_rule_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rules/serialize_rule_row.md)
- [query_rows_truncates_variable_property_values_to_microsoft_limit](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/query_rows_truncates_variable_property_values_to_microsoft_limit.md)
- [find_row_uses_windowed_content_table_rows_with_global_position](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/find_row_uses_windowed_content_table_rows_with_global_position.md)
- [find_row_beginning_origin_keeps_windowed_global_position](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/find_row_beginning_origin_keeps_windowed_global_position.md)
- [find_row_beginning_origin_falls_back_when_complete_rows_are_loaded](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/find_row_beginning_origin_falls_back_when_complete_rows_are_loaded.md)
- [captured_calendar_table_query_rows_projects_exact_requested_property_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/captured_calendar_table_query_rows_projects_exact_requested_property_row.md)
- [common_views_find_row_honors_restriction](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/common_views_find_row_honors_restriction.md)
- [contacts_contents_find_row_matches_display_name](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/contacts_contents_find_row_matches_display_name.md)
- [common_views_find_row_does_not_invent_default_compact_named_view](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/common_views_find_row_does_not_invent_default_compact_named_view.md)
- [common_views_find_row_does_not_invent_default_sent_to_named_view](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/common_views_find_row_does_not_invent_default_sent_to_named_view.md)
- [inbox_associated_exact_virtual_find_row_does_not_inject_a_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_exact_virtual_find_row_does_not_inject_a_row.md)
- [inbox_associated_find_row_returns_not_found_for_unstored_aggregation_config](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_find_row_returns_not_found_for_unstored_aggregation_config.md)
- [inbox_associated_find_row_does_not_invent_a_default_for_a_broad_startup_lookup](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_find_row_does_not_invent_a_default_for_a_broad_startup_lookup.md)
- [quick_step_associated_find_row_does_not_return_synthetic_custom_action](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/quick_step_associated_find_row_does_not_return_synthetic_custom_action.md)
- [contacts_associated_find_row_preserves_table_position_for_contact_link_timestamp](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/contacts_associated_find_row_preserves_table_position_for_contact_link_timestamp.md)
- [empty_conversation_action_settings_find_row_returns_not_found](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/empty_conversation_action_settings_find_row_returns_not_found.md)
- [conversation_action_settings_find_row_honors_restriction](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/conversation_action_settings_find_row_honors_restriction.md)
- [inbox_associated_exact_configuration_find_row_uses_sort_order](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_exact_configuration_find_row_uses_sort_order.md)
- [inbox_associated_broad_configuration_find_row_projects_single_followup_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_broad_configuration_find_row_projects_single_followup_row.md)
- [inbox_associated_exact_named_view_find_row_respects_existing_table_restriction](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_exact_named_view_find_row_respects_existing_table_restriction.md)
- [inbox_associated_find_row_followup_uses_the_original_rowset](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_find_row_followup_uses_the_original_rowset.md)
- [inbox_associated_find_row_can_return_a_persisted_extended_rule_message](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_find_row_can_return_a_persisted_extended_rule_message.md)
- [inbox_associated_find_row_response_for_message_class](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_associated_find_row_response_for_message_class.md)
- [contact_folder_associated_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/contact_folder_associated_find_row_response.md)
- [utf16_position](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/utf16_position.md)
- [utf16_occurrences](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/utf16_occurrences.md)
- [microsoft_contents_table_query_find_and_expand_require_set_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/microsoft_contents_table_query_find_and_expand_require_set_columns.md)
- [connect_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/connect_response.md)
- [connect_body_debug_summary_decodes_fields](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/connect_body_debug_summary_decodes_fields.md)
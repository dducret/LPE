---
type: Rust Function
title: rop_get_properties_specific_response
resource: crates/lpe-exchange/src/mapi/rop.rs#L49-L67
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/calendar_folder_getprops_trace_summarizes_response_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/journal_getprops_flags_absent_web_view_properties
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/inbox_getprops_flags_absent_retention_identity_properties
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/inbox_getprops_flags_binary_acl_member_name_as_absent
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/root_default_folder_getprops_uses_canonical_projection_not_setprops_state
  - functions/crates/lpe-exchange/src/mapi/rop/tests/get_properties_specific_preserves_values_and_flags_absent_message_deadlines
  - functions/crates/lpe-exchange/src/mapi/rop/tests/get_properties_specific_resolves_unspecified_modeled_message_properties
  - functions/crates/lpe-exchange/src/mapi/rop/tests/get_properties_specific_returns_not_enough_memory_for_size_limited_value
  - functions/crates/lpe-exchange/src/mapi/rop/tests/get_properties_specific_size_limit_preserves_unspecified_property_type
  - functions/crates/lpe-exchange/src/mapi/rop/tests/logon_getprops_projects_extended_rule_size_limit
  - functions/crates/lpe-exchange/src/mapi/rop/tests/associated_config_absent_optional_getprops_returns_not_found
  - functions/crates/lpe-exchange/src/mapi/rop/tests/persisted_named_view_getprops_does_not_project_missing_0e0b
  - functions/crates/lpe-exchange/src/mapi/rop/tests/persisted_message_list_settings_getprops_projects_exchange_private_entry_id
  - functions/crates/lpe-exchange/src/mapi/rop/tests/associated_config_getprops_rejects_default_from_wrong_folder
  - functions/crates/lpe-exchange/src/mapi/rop/tests/folder_default_named_view_getprops_rejects_unpersisted_message
  - functions/crates/lpe-exchange/src/mapi/rop/tests/calendar_event_getprops_specific_projects_visible_event
  - functions/crates/lpe-exchange/src/mapi/rop/tests/conversation_action_getprops_rejects_default_from_wrong_folder
  - functions/crates/lpe-exchange/src/mapi/rop/tests/delegate_freebusy_getprops_rejects_message_from_wrong_folder
  - functions/crates/lpe-exchange/src/mapi/rop/tests/microsoft_oxcdata_property_row_example_streams_oversized_body
  - functions/crates/lpe-exchange/src/mapi/rop/tests/saved_message_handle_getprops_keeps_batch_email_and_durable_identity
  - functions/crates/lpe-exchange/src/mapi/rop/tests/persisted_message_getprops_returns_body_values
  - functions/crates/lpe-exchange/src/mapi/rop/tests/saved_associated_config_getprops_uses_same_batch_saved_message
  - functions/crates/lpe-exchange/src/mapi/rop/tests/saved_umolk_associated_config_getprops_reports_missing_0e0b_not_found
  - functions/crates/lpe-exchange/src/mapi/rop/tests/umolk_associated_config_property_burst_reports_absent_values_not_found
  - functions/crates/lpe-exchange/src/mapi/rop/tests/umolk_trace_property_burst_does_not_fabricate_optional_standard_values
  - functions/crates/lpe-exchange/src/mapi/rop/tests/contacts_helper_associated_getprops_projects_empty_modeled_values
  - functions/crates/lpe-exchange/src/mapi/rop/tests/property_row_kind_reports_fallback_defaults_as_flagged
  - functions/crates/lpe-exchange/src/mapi/rop/tests/inbox_getprops_captured_unpersisted_folder_values_are_absent
  - functions/crates/lpe-exchange/src/mapi/rop/tests/inbox_getprops_preserves_explicit_archive_and_offline_reminders_values
  - functions/crates/lpe-exchange/src/mapi/rop/tests/newly_created_associated_message_getprops_uses_new_message_contract
  - functions/crates/lpe-exchange/src/mapi/rop/tests/undocumented_folder_binary_120c_returns_empty_binary
  - functions/crates/lpe-exchange/src/mapi/rop/tests/folder_default_view_entry_id_resolves_persisted_named_view_fai
  - functions/crates/lpe-exchange/src/mapi/rop/tests/contacts_search_getprops_content_count_matches_projected_results
---

# Signature

`pub(in crate::mapi) fn rop_get_properties_specific_response( request: &RopRequest, object: Option<&MapiObject>, principal: &AccountPrincipal, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, ) -> Vec<u8>`

# Calls

- [rop_get_properties_specific_response_with_custom](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom.md)

# Called by

- [calendar_folder_getprops_trace_summarizes_response_contract](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/calendar_folder_getprops_trace_summarizes_response_contract.md)
- [journal_getprops_flags_absent_web_view_properties](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/journal_getprops_flags_absent_web_view_properties.md)
- [inbox_getprops_flags_absent_retention_identity_properties](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/inbox_getprops_flags_absent_retention_identity_properties.md)
- [inbox_getprops_flags_binary_acl_member_name_as_absent](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/inbox_getprops_flags_binary_acl_member_name_as_absent.md)
- [root_default_folder_getprops_uses_canonical_projection_not_setprops_state](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/root_default_folder_getprops_uses_canonical_projection_not_setprops_state.md)
- [get_properties_specific_preserves_values_and_flags_absent_message_deadlines](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/get_properties_specific_preserves_values_and_flags_absent_message_deadlines.md)
- [get_properties_specific_resolves_unspecified_modeled_message_properties](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/get_properties_specific_resolves_unspecified_modeled_message_properties.md)
- [get_properties_specific_returns_not_enough_memory_for_size_limited_value](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/get_properties_specific_returns_not_enough_memory_for_size_limited_value.md)
- [get_properties_specific_size_limit_preserves_unspecified_property_type](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/get_properties_specific_size_limit_preserves_unspecified_property_type.md)
- [logon_getprops_projects_extended_rule_size_limit](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/logon_getprops_projects_extended_rule_size_limit.md)
- [associated_config_absent_optional_getprops_returns_not_found](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/associated_config_absent_optional_getprops_returns_not_found.md)
- [persisted_named_view_getprops_does_not_project_missing_0e0b](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/persisted_named_view_getprops_does_not_project_missing_0e0b.md)
- [persisted_message_list_settings_getprops_projects_exchange_private_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/persisted_message_list_settings_getprops_projects_exchange_private_entry_id.md)
- [associated_config_getprops_rejects_default_from_wrong_folder](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/associated_config_getprops_rejects_default_from_wrong_folder.md)
- [folder_default_named_view_getprops_rejects_unpersisted_message](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/folder_default_named_view_getprops_rejects_unpersisted_message.md)
- [calendar_event_getprops_specific_projects_visible_event](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/calendar_event_getprops_specific_projects_visible_event.md)
- [conversation_action_getprops_rejects_default_from_wrong_folder](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/conversation_action_getprops_rejects_default_from_wrong_folder.md)
- [delegate_freebusy_getprops_rejects_message_from_wrong_folder](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/delegate_freebusy_getprops_rejects_message_from_wrong_folder.md)
- [microsoft_oxcdata_property_row_example_streams_oversized_body](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/microsoft_oxcdata_property_row_example_streams_oversized_body.md)
- [saved_message_handle_getprops_keeps_batch_email_and_durable_identity](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/saved_message_handle_getprops_keeps_batch_email_and_durable_identity.md)
- [persisted_message_getprops_returns_body_values](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/persisted_message_getprops_returns_body_values.md)
- [saved_associated_config_getprops_uses_same_batch_saved_message](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/saved_associated_config_getprops_uses_same_batch_saved_message.md)
- [saved_umolk_associated_config_getprops_reports_missing_0e0b_not_found](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/saved_umolk_associated_config_getprops_reports_missing_0e0b_not_found.md)
- [umolk_associated_config_property_burst_reports_absent_values_not_found](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/umolk_associated_config_property_burst_reports_absent_values_not_found.md)
- [umolk_trace_property_burst_does_not_fabricate_optional_standard_values](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/umolk_trace_property_burst_does_not_fabricate_optional_standard_values.md)
- [contacts_helper_associated_getprops_projects_empty_modeled_values](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/contacts_helper_associated_getprops_projects_empty_modeled_values.md)
- [property_row_kind_reports_fallback_defaults_as_flagged](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/property_row_kind_reports_fallback_defaults_as_flagged.md)
- [inbox_getprops_captured_unpersisted_folder_values_are_absent](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/inbox_getprops_captured_unpersisted_folder_values_are_absent.md)
- [inbox_getprops_preserves_explicit_archive_and_offline_reminders_values](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/inbox_getprops_preserves_explicit_archive_and_offline_reminders_values.md)
- [newly_created_associated_message_getprops_uses_new_message_contract](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/newly_created_associated_message_getprops_uses_new_message_contract.md)
- [undocumented_folder_binary_120c_returns_empty_binary](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/undocumented_folder_binary_120c_returns_empty_binary.md)
- [folder_default_view_entry_id_resolves_persisted_named_view_fai](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/folder_default_view_entry_id_resolves_persisted_named_view_fai.md)
- [contacts_search_getprops_content_count_matches_projected_results](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/contacts_search_getprops_content_count_matches_projected_results.md)
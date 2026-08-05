---
type: Rust Function
title: format_outlook_view_handoff_table_contract
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views.rs#L185-L317
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/is_outlook_folder_table_debug_target
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id
  - functions/crates/lpe-exchange/src/mapi/tables/folders/collaboration_folder_message_class
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/advertised_special_folder_container_class
  - functions/crates/lpe-exchange/src/mapi/properties/views/default_common_views_named_view_id
  - functions/crates/lpe-exchange/src/mapi/properties/views/default_view_supported_folder
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/common_view_named_view_message_for_id
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/outlook_default_folder_named_view_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_default_folder_associated_named_view
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/default_folder_named_view_message
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_associated_table_rows
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_associated_row_id
  - functions/crates/lpe-exchange/src/mapi/properties/views/outlook_folder_view_definition
  - functions/crates/lpe-exchange/src/mapi/properties/views/view_descriptor_binary
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_view_descriptor_binary_summary
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/view_descriptor_runtime_property_tags
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/view_descriptor_comparable_selected_columns
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/missing_debug_property_tags
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar_contract/format_calendar_view_contract_fingerprint
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/common_views_table_contract_reports_no_unpersisted_named_view
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_find_row
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_open
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_set_columns
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_sort
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_restrict
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_query_rows
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_query_rows_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_seek_row
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_set_columns_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/inbox_view_handoff_table_contract_reports_no_unpersisted_default_view
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/sent_view_handoff_table_contract_reports_no_unpersisted_default_view
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/junk_view_handoff_table_contract_reports_no_unpersisted_default_view
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/quick_step_view_handoff_table_contract_reports_unsupported_default_view
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/contacts_view_handoff_table_contract_reports_no_unpersisted_default_view
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/calendar_view_handoff_table_contract_reports_client_normal_view
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/task_note_journal_handoff_contracts_report_standard_visible_columns
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/calendar_associated_view_handoff_uses_client_normal_view
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/calendar_normal_view_handoff_does_not_claim_server_descriptor
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/messages_view_handoff_does_not_invent_drafts_descriptor
---

# Signature

`pub(in crate::mapi::dispatch) fn format_outlook_view_handoff_table_contract( folder_id: u64, associated: bool, columns: &[u32], snapshot: &MapiMailStoreSnapshot, ) -> String`

# Calls

- [is_outlook_folder_table_debug_target](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/is_outlook_folder_table_debug_target.md)
- [collaboration_folder_for_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id.md)
- [collaboration_folder_message_class](../../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/collaboration_folder_message_class.md)
- [advertised_special_folder_container_class](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/advertised_special_folder_container_class.md)
- [default_common_views_named_view_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/default_common_views_named_view_id.md)
- [default_view_supported_folder](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/default_view_supported_folder.md)
- [common_view_named_view_message_for_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/common_view_named_view_message_for_id.md)
- [outlook_default_folder_named_view_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/outlook_default_folder_named_view_id.md)
- [debug_default_folder_associated_named_view](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_default_folder_associated_named_view.md)
- [default_folder_named_view_message](../../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/default_folder_named_view_message.md)
- [debug_associated_table_rows](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_associated_table_rows.md)
- [debug_associated_row_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_associated_row_id.md)
- [outlook_folder_view_definition](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/outlook_folder_view_definition.md)
- [view_descriptor_binary](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/view_descriptor_binary.md)
- [format_view_descriptor_binary_summary](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_view_descriptor_binary_summary.md)
- [view_descriptor_runtime_property_tags](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/view_descriptor_runtime_property_tags.md)
- [view_descriptor_comparable_selected_columns](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/view_descriptor_comparable_selected_columns.md)
- [missing_debug_property_tags](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/missing_debug_property_tags.md)

# Called by

- [format_calendar_view_contract_fingerprint](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/calendar_contract/format_calendar_view_contract_fingerprint.md)
- [common_views_table_contract_reports_no_unpersisted_named_view](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/common_views_table_contract_reports_no_unpersisted_named_view.md)
- [log_outlook_contents_table_find_row](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_find_row.md)
- [log_outlook_contents_table_open](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_open.md)
- [log_outlook_contents_table_set_columns](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_set_columns.md)
- [log_outlook_contents_table_sort](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_sort.md)
- [log_outlook_contents_table_restrict](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_restrict.md)
- [log_outlook_contents_table_query_rows](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_query_rows.md)
- [log_outlook_contents_table_query_rows_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_query_rows_response.md)
- [log_outlook_contents_table_seek_row](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_seek_row.md)
- [append_set_columns_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_set_columns_response.md)
- [inbox_view_handoff_table_contract_reports_no_unpersisted_default_view](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/inbox_view_handoff_table_contract_reports_no_unpersisted_default_view.md)
- [sent_view_handoff_table_contract_reports_no_unpersisted_default_view](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/sent_view_handoff_table_contract_reports_no_unpersisted_default_view.md)
- [junk_view_handoff_table_contract_reports_no_unpersisted_default_view](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/junk_view_handoff_table_contract_reports_no_unpersisted_default_view.md)
- [quick_step_view_handoff_table_contract_reports_unsupported_default_view](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/quick_step_view_handoff_table_contract_reports_unsupported_default_view.md)
- [contacts_view_handoff_table_contract_reports_no_unpersisted_default_view](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/contacts_view_handoff_table_contract_reports_no_unpersisted_default_view.md)
- [calendar_view_handoff_table_contract_reports_client_normal_view](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/calendar_view_handoff_table_contract_reports_client_normal_view.md)
- [task_note_journal_handoff_contracts_report_standard_visible_columns](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/task_note_journal_handoff_contracts_report_standard_visible_columns.md)
- [calendar_associated_view_handoff_uses_client_normal_view](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/calendar_associated_view_handoff_uses_client_normal_view.md)
- [calendar_normal_view_handoff_does_not_claim_server_descriptor](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/calendar_normal_view_handoff_does_not_claim_server_descriptor.md)
- [messages_view_handoff_does_not_invent_drafts_descriptor](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/messages_view_handoff_does_not_invent_drafts_descriptor.md)
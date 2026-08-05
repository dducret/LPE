---
type: Rust Function
title: format_inbox_view_descriptor_behavior_contract
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views.rs#L345-L426
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_advertised_default_named_view
  - functions/crates/lpe-exchange/src/mapi/properties/views/outlook_folder_view_definition
  - functions/crates/lpe-exchange/src/mapi/properties/views/view_descriptor_binary
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/view_descriptor_runtime_property_tags
  - functions/crates/lpe-exchange/src/mapi/sync/scope/emails_for_folder
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_email
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_emails
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/select_query_window
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/view_descriptor_comparable_selected_columns
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/missing_debug_property_tags
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/message/normal_message_debug_property_value
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/message/format_normal_message_debug_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_query_rows
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_mapi_query_position_debug
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/inbox_descriptor_behavior_contract_requires_persisted_view_after_early_release
---

# Signature

`pub(in crate::mapi::dispatch) fn format_inbox_view_descriptor_behavior_contract( folder_id: u64, associated: bool, position: usize, forward_read: bool, row_count: usize, sort_orders: &[MapiSortOrder], restriction: Option<&MapiRestriction>, columns: &[u32], mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, ) -> String`

# Calls

- [debug_advertised_default_named_view](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_advertised_default_named_view.md)
- [outlook_folder_view_definition](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/outlook_folder_view_definition.md)
- [view_descriptor_binary](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/view_descriptor_binary.md)
- [view_descriptor_runtime_property_tags](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/view_descriptor_runtime_property_tags.md)
- [emails_for_folder](../../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/scope/emails_for_folder.md)
- [restriction_matches_email](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_email.md)
- [sort_emails](../../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_emails.md)
- [select_query_window](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/select_query_window.md)
- [view_descriptor_comparable_selected_columns](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/view_descriptor_comparable_selected_columns.md)
- [missing_debug_property_tags](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/missing_debug_property_tags.md)
- [normal_message_debug_property_value](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/message/normal_message_debug_property_value.md)
- [format_normal_message_debug_value](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/message/format_normal_message_debug_value.md)

# Called by

- [log_outlook_contents_table_query_rows](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_query_rows.md)
- [log_mapi_query_position_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_mapi_query_position_debug.md)
- [inbox_descriptor_behavior_contract_requires_persisted_view_after_early_release](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/inbox_descriptor_behavior_contract_requires_persisted_view_after_early_release.md)
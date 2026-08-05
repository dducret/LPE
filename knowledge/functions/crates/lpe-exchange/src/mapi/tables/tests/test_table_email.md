---
type: Rust Function
title: test_table_email
resource: crates/lpe-exchange/src/mapi/tables/tests.rs#L8660-L8727
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/tests/sent_default_view_sort_orders_by_client_submit_time
  - functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_contents_invariant_accepts_message_identity_columns
  - functions/crates/lpe-exchange/src/mapi/tables/tests/bookmark_seek_does_not_mark_sparse_window_unknown_row_deleted
  - functions/crates/lpe-exchange/src/mapi/tables/tests/query_position_counts_categorized_content_rows
  - functions/crates/lpe-exchange/src/mapi/tables/tests/categorized_keywords_project_multivalue_instances_and_table_row_metadata
  - functions/crates/lpe-exchange/src/mapi/tables/tests/message_table_row_flags_absent_deadline_expiry_and_recall_times
  - functions/crates/lpe-exchange/src/mapi/tables/tests/message_row_projects_containing_folder_ids
  - functions/crates/lpe-exchange/src/mapi/tables/tests/normal_contents_property_row_uses_durable_message_identity
  - functions/crates/lpe-exchange/src/mapi/tables/tests/draft_message_row_projects_mf_unsent_from_canonical_mailbox_state
  - functions/crates/lpe-exchange/src/mapi/tables/tests/message_row_client_submit_time_falls_back_to_received_time
  - functions/crates/lpe-exchange/src/mapi/tables/tests/normal_message_row_projects_outlook_inbox_view_columns
  - functions/crates/lpe-exchange/src/mapi/tables/tests/normal_inbox_query_rows_projects_sender_and_delivery_time
  - functions/crates/lpe-exchange/src/mapi/tables/tests/categorized_and_deleted_message_rows_keep_long_term_entry_ids
  - functions/crates/lpe-exchange/src/mapi/tables/tests/normal_message_row_projects_microsoft_view_descriptor_string8_columns
  - functions/crates/lpe-exchange/src/mapi/tables/tests/microsoft_categorized_expand_collapse_report_current_state_errors
---

# Signature

`fn test_table_email(id: Uuid, mailbox_id: Uuid, subject: &str) -> JmapEmail`

# Called by

- [sent_default_view_sort_orders_by_client_submit_time](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/sent_default_view_sort_orders_by_client_submit_time.md)
- [inbox_contents_invariant_accepts_message_identity_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/inbox_contents_invariant_accepts_message_identity_columns.md)
- [bookmark_seek_does_not_mark_sparse_window_unknown_row_deleted](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/bookmark_seek_does_not_mark_sparse_window_unknown_row_deleted.md)
- [query_position_counts_categorized_content_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/query_position_counts_categorized_content_rows.md)
- [categorized_keywords_project_multivalue_instances_and_table_row_metadata](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/categorized_keywords_project_multivalue_instances_and_table_row_metadata.md)
- [message_table_row_flags_absent_deadline_expiry_and_recall_times](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/message_table_row_flags_absent_deadline_expiry_and_recall_times.md)
- [message_row_projects_containing_folder_ids](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/message_row_projects_containing_folder_ids.md)
- [normal_contents_property_row_uses_durable_message_identity](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/normal_contents_property_row_uses_durable_message_identity.md)
- [draft_message_row_projects_mf_unsent_from_canonical_mailbox_state](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/draft_message_row_projects_mf_unsent_from_canonical_mailbox_state.md)
- [message_row_client_submit_time_falls_back_to_received_time](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/message_row_client_submit_time_falls_back_to_received_time.md)
- [normal_message_row_projects_outlook_inbox_view_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/normal_message_row_projects_outlook_inbox_view_columns.md)
- [normal_inbox_query_rows_projects_sender_and_delivery_time](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/normal_inbox_query_rows_projects_sender_and_delivery_time.md)
- [categorized_and_deleted_message_rows_keep_long_term_entry_ids](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/categorized_and_deleted_message_rows_keep_long_term_entry_ids.md)
- [normal_message_row_projects_microsoft_view_descriptor_string8_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/normal_message_row_projects_microsoft_view_descriptor_string8_columns.md)
- [microsoft_categorized_expand_collapse_report_current_state_errors](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/microsoft_categorized_expand_collapse_report_current_state_errors.md)
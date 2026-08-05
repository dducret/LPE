---
type: Rust Function
title: emails_for_folder
resource: crates/lpe-exchange/src/mapi/sync/scope.rs#L63-L72
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/sync/email_matches_folder
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_inbox_view_descriptor_behavior_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_fast_transfer_source_copy_messages_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_lifecycle/format_visible_inbox_first_row_projection_audit
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_normal_message_query_row_summary
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_normal_message_find_row_failure_candidates
  - functions/crates/lpe-exchange/src/mapi/sync/sync_emails_for
  - functions/crates/lpe-exchange/src/mapi/sync/fast_transfer_manifest_for_object
  - functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/collapse/expanded_categorized_rows
  - functions/crates/lpe-exchange/src/mapi/tables/counts/folder_message_count
  - functions/crates/lpe-exchange/src/mapi/tables/counts/table_position_and_count
  - functions/crates/lpe-exchange/src/mapi/tables/deleted_items/deleted_items_content_rows
  - functions/crates/lpe-exchange/src/mapi/tables/diagnostics/outlook_bootstrap_row_invariant_summaries
  - functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner
  - functions/crates/lpe-exchange/src/mapi/tables/row_keys/table_row_keys
---

# Signature

`pub(in crate::mapi) fn emails_for_folder<'a>( folder_id: u64, mailboxes: &[JmapMailbox], emails: &'a [JmapEmail], ) -> Vec<&'a JmapEmail>`

# Calls

- [email_matches_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/email_matches_folder.md)

# Called by

- [format_inbox_view_descriptor_behavior_contract](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_inbox_view_descriptor_behavior_contract.md)
- [append_fast_transfer_source_copy_messages_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_fast_transfer_source_copy_messages_response.md)
- [format_visible_inbox_first_row_projection_audit](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_lifecycle/format_visible_inbox_first_row_projection_audit.md)
- [format_normal_message_query_row_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_normal_message_query_row_summary.md)
- [format_normal_message_find_row_failure_candidates](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_normal_message_find_row_failure_candidates.md)
- [sync_emails_for](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/sync_emails_for.md)
- [fast_transfer_manifest_for_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/fast_transfer_manifest_for_object.md)
- [rop_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)
- [expanded_categorized_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collapse/expanded_categorized_rows.md)
- [folder_message_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/folder_message_count.md)
- [table_position_and_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/table_position_and_count.md)
- [deleted_items_content_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/deleted_items/deleted_items_content_rows.md)
- [outlook_bootstrap_row_invariant_summaries](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/diagnostics/outlook_bootstrap_row_invariant_summaries.md)
- [rop_query_rows_response_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/query_rows/rop_query_rows_response_inner.md)
- [table_row_keys](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_keys/table_row_keys.md)
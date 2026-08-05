---
type: Rust Function
title: format_normal_message_find_row_failure_candidates
resource: crates/lpe-exchange/src/mapi/dispatch/tables.rs#L856-L927
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/sync/scope/emails_for_folder
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_email
  - functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_emails
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/candidate_find_row_debug_tags
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/restriction
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/select_query_window
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/message/normal_message_debug_property_value
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/message/format_normal_message_debug_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_find_row
---

# Signature

`pub(super) fn format_normal_message_find_row_failure_candidates( folder_id: u64, position: usize, find_backward: bool, request: &RopRequest, table_restriction: Option<&MapiRestriction>, sort_orders: &[MapiSortOrder], selected_columns: &[u32], restriction_property_tags: &[u32], mailboxes: &[JmapMailbox], emails: &[JmapEmail], ) -> String`

# Calls

- [emails_for_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/scope/emails_for_folder.md)
- [restriction_matches_email](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_email.md)
- [sort_emails](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/sorting/sort_emails.md)
- [candidate_find_row_debug_tags](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/candidate_find_row_debug_tags.md)
- [restriction](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/restriction.md)
- [select_query_window](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/select_query_window.md)
- [normal_message_debug_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/message/normal_message_debug_property_value.md)
- [format_normal_message_debug_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/message/format_normal_message_debug_value.md)

# Called by

- [log_outlook_contents_table_find_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/table_queries/log_outlook_contents_table_find_row.md)
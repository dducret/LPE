---
type: Rust Function
title: restriction_matches_email
resource: crates/lpe-exchange/src/mapi/properties.rs#L214-L219
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_email_with_attachments
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_inbox_view_descriptor_behavior_contract
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_lifecycle/format_visible_inbox_first_row_projection_audit
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_normal_message_query_row_summary
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_normal_message_find_row_failure_candidates
  - functions/crates/lpe-exchange/src/mapi/tables/counts/table_position_and_count
  - functions/crates/lpe-exchange/src/mapi/tables/search_folders/todo_search_content_rows
  - functions/crates/lpe-exchange/src/mapi/tables/search_folders/reminder_search_content_rows
  - functions/crates/lpe-exchange/src/mapi/tables/search_folders/search_content_row_matches
---

# Signature

`pub(in crate::mapi) fn restriction_matches_email( restriction: Option<&MapiRestriction>, email: &JmapEmail, ) -> bool`

# Calls

- [restriction_matches_email_with_attachments](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_email_with_attachments.md)

# Called by

- [format_inbox_view_descriptor_behavior_contract](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/common_views/format_inbox_view_descriptor_behavior_contract.md)
- [format_visible_inbox_first_row_projection_audit](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_lifecycle/format_visible_inbox_first_row_projection_audit.md)
- [format_normal_message_query_row_summary](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_normal_message_query_row_summary.md)
- [format_normal_message_find_row_failure_candidates](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_normal_message_find_row_failure_candidates.md)
- [table_position_and_count](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/table_position_and_count.md)
- [todo_search_content_rows](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/search_folders/todo_search_content_rows.md)
- [reminder_search_content_rows](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/search_folders/reminder_search_content_rows.md)
- [search_content_row_matches](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/search_folders/search_content_row_matches.md)
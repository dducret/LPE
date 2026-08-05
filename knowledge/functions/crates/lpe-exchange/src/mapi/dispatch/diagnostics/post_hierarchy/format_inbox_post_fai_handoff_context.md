---
type: Rust Function
title: format_inbox_post_fai_handoff_context
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy.rs#L599-L641
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/log_post_common_views_handoff_execute_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/release/append_release_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_query_rows_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/inbox_post_fai_handoff_context_points_to_missing_contents_step
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/inbox_handoff_context_reports_visible_query_position_before_query_rows
---

# Signature

`pub(in crate::mapi) fn format_inbox_post_fai_handoff_context( state: &PostHierarchyActionState, ) -> String`

# Called by

- [log_post_common_views_handoff_execute_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/log_post_common_views_handoff_execute_response.md)
- [append_release_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/release/append_release_response.md)
- [append_query_rows_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_query_rows_response.md)
- [inbox_post_fai_handoff_context_points_to_missing_contents_step](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/inbox_post_fai_handoff_context_points_to_missing_contents_step.md)
- [inbox_handoff_context_reports_visible_query_position_before_query_rows](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/inbox_handoff_context_reports_visible_query_position_before_query_rows.md)
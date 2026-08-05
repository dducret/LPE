---
type: Rust Method
title: record_last_hierarchy_table_query_position_context
resource: crates/lpe-exchange/src/mapi/session.rs#L450-L508
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_view_failure_trace_event
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/append_table_control_response
  - functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_summary_exports_hierarchy_query_position_context
  - functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_summary_counts_hierarchy_query_position_after_visible_release
  - functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_summary_counts_hierarchy_query_position_after_visible_findrow_release
---

# Signature

`pub(in crate::mapi) fn record_last_hierarchy_table_query_position_context( &mut self, context: String, )`

# Calls

- [record_outlook_view_failure_trace_event](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_view_failure_trace_event.md)

# Called by

- [append_table_control_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/append_table_control_response.md)
- [post_hierarchy_summary_exports_hierarchy_query_position_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_summary_exports_hierarchy_query_position_context.md)
- [post_hierarchy_summary_counts_hierarchy_query_position_after_visible_release](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_summary_counts_hierarchy_query_position_after_visible_release.md)
- [post_hierarchy_summary_counts_hierarchy_query_position_after_visible_findrow_release](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_summary_counts_hierarchy_query_position_after_visible_findrow_release.md)
---
type: Rust Method
title: record_post_hierarchy_submit_attempt_context
resource: crates/lpe-exchange/src/mapi/session.rs#L528-L549
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/hierarchy_sync_completed
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_view_failure_trace_event
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_submit_message_response
  - functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_action_summary_records_submit_attempt_context
---

# Signature

`pub(in crate::mapi) fn record_post_hierarchy_submit_attempt_context( &mut self, context: String, )`

# Calls

- [hierarchy_sync_completed](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/hierarchy_sync_completed.md)
- [record_outlook_view_failure_trace_event](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_view_failure_trace_event.md)

# Called by

- [append_submit_message_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_submit_message_response.md)
- [post_hierarchy_action_summary_records_submit_attempt_context](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_action_summary_records_submit_attempt_context.md)
---
type: Rust Method
title: record_last_post_hierarchy_create_save_object_context
resource: crates/lpe-exchange/src/mapi/session.rs#L515-L526
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/hierarchy_sync_completed
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_view_failure_trace_event
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/append_pending_associated_config_save_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/append_pending_navigation_shortcut_save_response
  - functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_action_summary_records_last_create_save_object
---

# Signature

`pub(in crate::mapi) fn record_last_post_hierarchy_create_save_object_context( &mut self, context: String, )`

# Calls

- [hierarchy_sync_completed](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/hierarchy_sync_completed.md)
- [record_outlook_view_failure_trace_event](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/record_outlook_view_failure_trace_event.md)

# Called by

- [append_pending_associated_config_save_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/associated_config/append_pending_associated_config_save_response.md)
- [append_save_changes_message_route_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response.md)
- [append_pending_navigation_shortcut_save_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/append_pending_navigation_shortcut_save_response.md)
- [post_hierarchy_action_summary_records_last_create_save_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_action_summary_records_last_create_save_object.md)
---
type: Rust Method
title: record_logoff_after_hierarchy_completion
resource: crates/lpe-exchange/src/mapi/session.rs#L1093-L1097
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/hierarchy_sync_completed
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/release/append_release_response
  - functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_action_summary_records_execute_rops_and_client_actions
  - functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_action_summary_classifies_release_logoff_without_content_sync
---

# Signature

`pub(in crate::mapi) fn record_logoff_after_hierarchy_completion(&mut self)`

# Calls

- [hierarchy_sync_completed](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/hierarchy_sync_completed.md)

# Called by

- [append_release_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/release/append_release_response.md)
- [post_hierarchy_action_summary_records_execute_rops_and_client_actions](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_action_summary_records_execute_rops_and_client_actions.md)
- [post_hierarchy_action_summary_classifies_release_logoff_without_content_sync](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/tests/post_hierarchy_action_summary_classifies_release_logoff_without_content_sync.md)
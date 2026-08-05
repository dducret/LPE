---
type: Rust Function
title: log_post_hierarchy_release_events
resource: crates/lpe-exchange/src/mapi/dispatch/release.rs#L641-L768
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/transport/diagnostics/post_hierarchy_action_summary
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops
---

# Signature

`pub(super) fn log_post_hierarchy_release_events( principal: &AccountPrincipal, request_id: &str, request_rop_ids: &str, request_rop_names: &str, request_non_release_rops: &str, request_all_rops_are_release: bool, request_handle_count: usize, request_handle_table_summary: &str, session: &MapiSession, post_hierarchy_release_events: &[PostHierarchyReleaseDebugEvent], responses: &[u8], )`

# Calls

- [post_hierarchy_action_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/diagnostics/post_hierarchy_action_summary.md)

# Called by

- [execute_rops](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops.md)
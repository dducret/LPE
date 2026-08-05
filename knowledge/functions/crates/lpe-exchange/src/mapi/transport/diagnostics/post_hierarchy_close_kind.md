---
type: Rust Function
title: post_hierarchy_close_kind
resource: crates/lpe-exchange/src/mapi/transport/diagnostics.rs#L561-L653
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/transport/diagnostics/visible_inbox_release_without_query_rows_observed
  called_by:
  - functions/crates/lpe-exchange/src/mapi/transport/diagnostics/post_hierarchy_action_summary
---

# Signature

`pub(in crate::mapi) fn post_hierarchy_close_kind( actions: &PostHierarchyActionState, disconnect_client_initiated: bool, ) -> &'static str`

# Calls

- [visible_inbox_release_without_query_rows_observed](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/diagnostics/visible_inbox_release_without_query_rows_observed.md)

# Called by

- [post_hierarchy_action_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/diagnostics/post_hierarchy_action_summary.md)
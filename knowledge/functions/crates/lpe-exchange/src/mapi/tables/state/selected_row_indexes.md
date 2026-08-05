---
type: Rust Function
title: selected_row_indexes
resource: crates/lpe-exchange/src/mapi/tables/state.rs#L114-L129
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/diagnostics/outlook_bootstrap_row_invariant_summaries
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/log_sync_issues_hierarchy_query_rows
---

# Signature

`pub(super) fn selected_row_indexes( row_len: usize, start_position: usize, forward_read: bool, requested_row_count: usize, ) -> Vec<usize>`

# Called by

- [outlook_bootstrap_row_invariant_summaries](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/diagnostics/outlook_bootstrap_row_invariant_summaries.md)
- [log_sync_issues_hierarchy_query_rows](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/log_sync_issues_hierarchy_query_rows.md)
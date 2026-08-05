---
type: Rust Function
title: truncate_query_diff
resource: crates/lpe-jmap/src/state.rs#L647-L674
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/state/apply_query_changes
  called_by:
  - functions/crates/lpe-jmap/src/state/compute_query_diff
  - functions/crates/lpe-jmap/src/state/compute_query_diff_with_reorders
---

# Signature

`fn truncate_query_diff( previous_ids: &[String], current_ids: &[String], removed: &mut Vec<String>, added: &mut Vec<Value>, max_changes: Option<u64>, ) -> QueryDiff`

# Calls

- [apply_query_changes](../../../../../functions/crates/lpe-jmap/src/state/apply_query_changes.md)

# Called by

- [compute_query_diff](../../../../../functions/crates/lpe-jmap/src/state/compute_query_diff.md)
- [compute_query_diff_with_reorders](../../../../../functions/crates/lpe-jmap/src/state/compute_query_diff_with_reorders.md)
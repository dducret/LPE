---
type: Rust Function
title: compute_query_diff_with_reorders
resource: crates/lpe-jmap/src/state.rs#L702-L748
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-jmap/src/state/truncate_query_diff
  called_by:
  - functions/crates/lpe-jmap/src/state/query_diff_for_kind
---

# Signature

`pub(crate) fn compute_query_diff_with_reorders( previous_ids: &[String], current_ids: &[String], max_changes: Option<u64>, ) -> QueryDiff`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [truncate_query_diff](../../../../../functions/crates/lpe-jmap/src/state/truncate_query_diff.md)

# Called by

- [query_diff_for_kind](../../../../../functions/crates/lpe-jmap/src/state/query_diff_for_kind.md)
---
type: Rust Function
title: apply_query_changes
resource: crates/lpe-jmap/src/state.rs#L676-L700
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-jmap/src/state/truncate_query_diff
---

# Signature

`fn apply_query_changes( previous_ids: &[String], removed: &[String], added: &[Value], ) -> Vec<String>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [truncate_query_diff](../../../../../functions/crates/lpe-jmap/src/state/truncate_query_diff.md)
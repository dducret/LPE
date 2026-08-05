---
type: Rust Function
title: assert_plan_uses_index
resource: crates/lpe-storage/tests/runtime_schema_drift.rs#L2770-L2776
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_index_plan_paths
---

# Signature

`fn assert_plan_uses_index(label: &str, plan: &str, index_name: &str) -> Result<()>`

# Called by

- [exercise_index_plan_paths](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_index_plan_paths.md)
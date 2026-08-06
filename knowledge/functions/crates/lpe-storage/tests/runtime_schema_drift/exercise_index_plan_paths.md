---
type: Rust Function
title: exercise_index_plan_paths
resource: crates/lpe-storage/tests/runtime_schema_drift.rs#L2694-L2842
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-core/src/sieve/context
  - functions/crates/lpe-storage/tests/runtime_schema_drift/explain_rows
  - functions/crates/lpe-storage/tests/runtime_schema_drift/assert_plan_uses_index
  called_by:
  - functions/crates/lpe-storage/tests/runtime_schema_drift/run_runtime_drift_validation
---

# Signature

`async fn exercise_index_plan_paths( pool: &PgPool, fixture: &RuntimeFixture, submitted: &SubmittedMessage, ) -> Result<()>`

# Calls

- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [context](../../../../../functions/crates/lpe-core/src/sieve/context.md)
- [explain_rows](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/explain_rows.md)
- [assert_plan_uses_index](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/assert_plan_uses_index.md)

# Called by

- [run_runtime_drift_validation](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/run_runtime_drift_validation.md)
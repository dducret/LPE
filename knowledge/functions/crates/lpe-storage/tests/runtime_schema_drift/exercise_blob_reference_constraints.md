---
type: Rust Function
title: exercise_blob_reference_constraints
resource: crates/lpe-storage/tests/runtime_schema_drift.rs#L336-L617
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-core/src/sieve/context
  - functions/crates/lpe-storage/tests/runtime_schema_drift/expect_constraint_failure
  - functions/crates/lpe-storage/tests/runtime_schema_drift/hex64
  called_by:
  - functions/crates/lpe-storage/tests/runtime_schema_drift/run_runtime_drift_validation
---

# Signature

`async fn exercise_blob_reference_constraints(pool: &PgPool) -> Result<()>`

# Calls

- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [context](../../../../../functions/crates/lpe-core/src/sieve/context.md)
- [expect_constraint_failure](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/expect_constraint_failure.md)
- [hex64](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/hex64.md)

# Called by

- [run_runtime_drift_validation](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/run_runtime_drift_validation.md)
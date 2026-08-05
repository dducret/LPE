---
type: Rust Function
title: exercise_submission_cancellation_path
resource: crates/lpe-storage/tests/runtime_schema_drift.rs#L2406-L2519
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_submission_path
  - functions/crates/lpe-core/src/sieve/context
  called_by:
  - functions/crates/lpe-storage/tests/runtime_schema_drift/run_runtime_drift_validation
---

# Signature

`async fn exercise_submission_cancellation_path( storage: &Storage, pool: &PgPool, fixture: &RuntimeFixture, ) -> Result<()>`

# Calls

- [exercise_submission_path](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_submission_path.md)
- [context](../../../../../functions/crates/lpe-core/src/sieve/context.md)

# Called by

- [run_runtime_drift_validation](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/run_runtime_drift_validation.md)
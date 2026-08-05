---
type: Rust Function
title: exercise_submission_path
resource: crates/lpe-storage/tests/runtime_schema_drift.rs#L2362-L2404
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/context
  called_by:
  - functions/crates/lpe-storage/tests/runtime_schema_drift/run_runtime_drift_validation
  - functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_submission_cancellation_path
---

# Signature

`async fn exercise_submission_path( storage: &Storage, fixture: &RuntimeFixture, ) -> Result<SubmittedMessage>`

# Calls

- [context](../../../../../functions/crates/lpe-core/src/sieve/context.md)

# Called by

- [run_runtime_drift_validation](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/run_runtime_drift_validation.md)
- [exercise_submission_cancellation_path](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/exercise_submission_cancellation_path.md)
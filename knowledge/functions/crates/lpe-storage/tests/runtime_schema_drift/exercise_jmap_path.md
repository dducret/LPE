---
type: Rust Function
title: exercise_jmap_path
resource: crates/lpe-storage/tests/runtime_schema_drift.rs#L2531-L2621
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/context
  called_by:
  - functions/crates/lpe-storage/tests/runtime_schema_drift/run_runtime_drift_validation
---

# Signature

`async fn exercise_jmap_path( storage: &Storage, fixture: &RuntimeFixture, submitted: Option<&SubmittedMessage>, ) -> Result<()>`

# Calls

- [context](../../../../../functions/crates/lpe-core/src/sieve/context.md)

# Called by

- [run_runtime_drift_validation](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/run_runtime_drift_validation.md)
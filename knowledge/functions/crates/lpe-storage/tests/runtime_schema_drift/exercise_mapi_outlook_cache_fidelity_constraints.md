---
type: Rust Function
title: exercise_mapi_outlook_cache_fidelity_constraints
resource: crates/lpe-storage/tests/runtime_schema_drift.rs#L1065-L1236
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-core/src/sieve/context
  - functions/crates/lpe-storage/tests/runtime_schema_drift/expect_constraint_failure
  called_by:
  - functions/crates/lpe-storage/tests/runtime_schema_drift/run_runtime_drift_validation
---

# Signature

`async fn exercise_mapi_outlook_cache_fidelity_constraints( pool: &PgPool, fixture: &RuntimeFixture, ) -> Result<()>`

# Calls

- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [context](../../../../../functions/crates/lpe-core/src/sieve/context.md)
- [expect_constraint_failure](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/expect_constraint_failure.md)

# Called by

- [run_runtime_drift_validation](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/run_runtime_drift_validation.md)
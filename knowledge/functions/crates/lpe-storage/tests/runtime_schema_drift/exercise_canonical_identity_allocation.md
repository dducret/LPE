---
type: Rust Function
title: exercise_canonical_identity_allocation
resource: crates/lpe-storage/tests/runtime_schema_drift.rs#L4082-L4264
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/context
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/tests/runtime_schema_drift/expect_constraint_failure
  called_by:
  - functions/crates/lpe-storage/tests/runtime_schema_drift/run_runtime_drift_validation
---

# Signature

`async fn exercise_canonical_identity_allocation( storage: &Storage, pool: &PgPool, fixture: &RuntimeFixture, ) -> Result<()>`

# Calls

- [context](../../../../../functions/crates/lpe-core/src/sieve/context.md)
- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [expect_constraint_failure](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/expect_constraint_failure.md)

# Called by

- [run_runtime_drift_validation](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/run_runtime_drift_validation.md)
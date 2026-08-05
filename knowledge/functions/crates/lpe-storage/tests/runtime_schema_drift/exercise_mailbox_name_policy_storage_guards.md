---
type: Rust Function
title: exercise_mailbox_name_policy_storage_guards
resource: crates/lpe-storage/tests/runtime_schema_drift.rs#L1666-L1881
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/tests/runtime_schema_drift/jmap_create_input
  - functions/crates/lpe-core/src/sieve/context
  - functions/crates/lpe-storage/tests/runtime_schema_drift/expect_anyhow_failure
  called_by:
  - functions/crates/lpe-storage/tests/runtime_schema_drift/run_runtime_drift_validation
---

# Signature

`async fn exercise_mailbox_name_policy_storage_guards( storage: &Storage, pool: &PgPool, fixture: &RuntimeFixture, ) -> Result<()>`

# Calls

- [jmap_create_input](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/jmap_create_input.md)
- [context](../../../../../functions/crates/lpe-core/src/sieve/context.md)
- [expect_anyhow_failure](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/expect_anyhow_failure.md)

# Called by

- [run_runtime_drift_validation](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/run_runtime_drift_validation.md)
---
type: Rust Function
title: exercise_mapi_special_folder_alias_constraints
resource: crates/lpe-storage/tests/runtime_schema_drift.rs#L2214-L2323
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/tests/runtime_schema_drift/mapi_source_key
  - functions/crates/lpe-storage/tests/runtime_schema_drift/insert_mapi_special_folder_alias
  - functions/crates/lpe-core/src/sieve/context
  - functions/crates/lpe-storage/tests/runtime_schema_drift/expect_constraint_failure
  called_by:
  - functions/crates/lpe-storage/tests/runtime_schema_drift/run_runtime_drift_validation
---

# Signature

`async fn exercise_mapi_special_folder_alias_constraints( pool: &PgPool, fixture: &RuntimeFixture, ) -> Result<()>`

# Calls

- [mapi_source_key](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/mapi_source_key.md)
- [insert_mapi_special_folder_alias](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/insert_mapi_special_folder_alias.md)
- [context](../../../../../functions/crates/lpe-core/src/sieve/context.md)
- [expect_constraint_failure](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/expect_constraint_failure.md)

# Called by

- [run_runtime_drift_validation](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/run_runtime_drift_validation.md)
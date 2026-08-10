---
type: Rust Function
title: exercise_change_log_cursor_constraints
resource: crates/lpe-storage/tests/runtime_schema_drift.rs#L2013-L2222
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/tests/runtime_schema_drift/expect_constraint_failure
  - functions/crates/lpe-activesync/src/tests/query
  - functions/tools/rca_outlook_connectivity_check/execute
  - functions/crates/lpe-core/src/sieve/context
  - functions/crates/lpe-storage/src/change/Storage/purge_expired_replay_rows
  called_by:
  - functions/crates/lpe-storage/tests/runtime_schema_drift/run_runtime_drift_validation
---

# Signature

`async fn exercise_change_log_cursor_constraints( storage: &Storage, pool: &PgPool, fixture: &RuntimeFixture, ) -> Result<()>`

# Calls

- [expect_constraint_failure](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/expect_constraint_failure.md)
- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [execute](../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)
- [context](../../../../../functions/crates/lpe-core/src/sieve/context.md)
- [purge_expired_replay_rows](../../../../../functions/crates/lpe-storage/src/change/Storage/purge_expired_replay_rows.md)

# Called by

- [run_runtime_drift_validation](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/run_runtime_drift_validation.md)
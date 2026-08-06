---
type: Rust Function
title: exercise_cross_account_jmap_copy_bcc_projection
resource: crates/lpe-storage/tests/runtime_schema_drift.rs#L2623-L2692
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/context
  - functions/crates/lpe-activesync/src/tests/query
  called_by:
  - functions/crates/lpe-storage/tests/runtime_schema_drift/run_runtime_drift_validation
---

# Signature

`async fn exercise_cross_account_jmap_copy_bcc_projection( storage: &Storage, pool: &PgPool, fixture: &RuntimeFixture, submitted: &SubmittedMessage, ) -> Result<()>`

# Calls

- [context](../../../../../functions/crates/lpe-core/src/sieve/context.md)
- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)

# Called by

- [run_runtime_drift_validation](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/run_runtime_drift_validation.md)
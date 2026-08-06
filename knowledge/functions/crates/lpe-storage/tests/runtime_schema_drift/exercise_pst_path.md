---
type: Rust Function
title: exercise_pst_path
resource: crates/lpe-storage/tests/runtime_schema_drift.rs#L3138-L3170
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/context
  - functions/crates/lpe-storage/src/pst/Storage/process_pending_pst_jobs
  called_by:
  - functions/crates/lpe-storage/tests/runtime_schema_drift/run_runtime_drift_validation
---

# Signature

`async fn exercise_pst_path(storage: &Storage, mailbox_id: Uuid) -> Result<()>`

# Calls

- [context](../../../../../functions/crates/lpe-core/src/sieve/context.md)
- [process_pending_pst_jobs](../../../../../functions/crates/lpe-storage/src/pst/Storage/process_pending_pst_jobs.md)

# Called by

- [run_runtime_drift_validation](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/run_runtime_drift_validation.md)
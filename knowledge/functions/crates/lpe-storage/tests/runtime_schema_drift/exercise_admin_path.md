---
type: Rust Function
title: exercise_admin_path
resource: crates/lpe-storage/tests/runtime_schema_drift.rs#L678-L773
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/context
  - functions/crates/lpe-storage/src/core/Storage/pool
  - functions/crates/lpe-storage/src/admin/dashboard/Storage/fetch_admin_dashboard
  called_by:
  - functions/crates/lpe-storage/tests/runtime_schema_drift/run_runtime_drift_validation
---

# Signature

`async fn exercise_admin_path(storage: &Storage) -> Result<()>`

# Calls

- [context](../../../../../functions/crates/lpe-core/src/sieve/context.md)
- [pool](../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)
- [fetch_admin_dashboard](../../../../../functions/crates/lpe-storage/src/admin/dashboard/Storage/fetch_admin_dashboard.md)

# Called by

- [run_runtime_drift_validation](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/run_runtime_drift_validation.md)
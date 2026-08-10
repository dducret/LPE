---
type: Rust Function
title: seed_platform_tenant
resource: crates/lpe-storage/tests/runtime_schema_drift.rs#L331-L344
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/tools/rca_outlook_connectivity_check/execute
  - functions/crates/lpe-core/src/sieve/context
  called_by:
  - functions/crates/lpe-storage/tests/runtime_schema_drift/run_runtime_drift_validation
---

# Signature

`async fn seed_platform_tenant(pool: &PgPool) -> Result<()>`

# Calls

- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [execute](../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)
- [context](../../../../../functions/crates/lpe-core/src/sieve/context.md)

# Called by

- [run_runtime_drift_validation](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/run_runtime_drift_validation.md)
---
type: Rust Function
title: recreate_source_schema
resource: crates/lpe-storage/tests/schema_051_update.rs#L242-L306
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/tools/rca_outlook_connectivity_check/execute
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-core/src/sieve/context
  called_by:
  - functions/crates/lpe-storage/tests/schema_051_update/run_update_scenarios
---

# Signature

`async fn recreate_source_schema(pool: &PgPool, schema_name: &str, version: &str) -> Result<()>`

# Calls

- [execute](../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)
- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [context](../../../../../functions/crates/lpe-core/src/sieve/context.md)

# Called by

- [run_update_scenarios](../../../../../functions/crates/lpe-storage/tests/schema_051_update/run_update_scenarios.md)
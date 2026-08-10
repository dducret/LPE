---
type: Rust Function
title: run_update_scenarios
resource: crates/lpe-storage/tests/schema_051_update.rs#L47-L222
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/tests/schema_051_update/recreate_source_schema
  - functions/crates/lpe-storage/tests/schema_051_update/assert_cache_fidelity_shape
  - functions/crates/lpe-storage/tests/schema_051_update/sql_for_schema
  - functions/crates/lpe-core/src/sieve/context
  - functions/crates/lpe-activesync/src/tests/query
  - functions/tools/rca_outlook_connectivity_check/execute
---

# Signature

`async fn run_update_scenarios(pool: &PgPool, schema_name: &str) -> Result<()>`

# Calls

- [recreate_source_schema](../../../../../functions/crates/lpe-storage/tests/schema_051_update/recreate_source_schema.md)
- [assert_cache_fidelity_shape](../../../../../functions/crates/lpe-storage/tests/schema_051_update/assert_cache_fidelity_shape.md)
- [sql_for_schema](../../../../../functions/crates/lpe-storage/tests/schema_051_update/sql_for_schema.md)
- [context](../../../../../functions/crates/lpe-core/src/sieve/context.md)
- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [execute](../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)
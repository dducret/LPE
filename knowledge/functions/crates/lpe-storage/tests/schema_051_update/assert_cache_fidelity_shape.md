---
type: Rust Function
title: assert_cache_fidelity_shape
resource: crates/lpe-storage/tests/schema_051_update.rs#L346-L408
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-core/src/sieve/context
  called_by:
  - functions/crates/lpe-storage/tests/schema_051_update/run_update_scenarios
---

# Signature

`async fn assert_cache_fidelity_shape( pool: &PgPool, schema_name: &str, expected_target: bool, ) -> Result<()>`

# Calls

- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [context](../../../../../functions/crates/lpe-core/src/sieve/context.md)

# Called by

- [run_update_scenarios](../../../../../functions/crates/lpe-storage/tests/schema_051_update/run_update_scenarios.md)
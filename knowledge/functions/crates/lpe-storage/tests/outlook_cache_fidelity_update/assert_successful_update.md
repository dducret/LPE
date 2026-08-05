---
type: Rust Function
title: assert_successful_update
resource: crates/lpe-storage/tests/outlook_cache_fidelity_update.rs#L242-L328
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-core/src/sieve/context
  called_by:
  - functions/crates/lpe-storage/tests/outlook_cache_fidelity_update/run_update_scenarios
---

# Signature

`async fn assert_successful_update(pool: &PgPool, schema_name: &str) -> Result<()>`

# Calls

- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [context](../../../../../functions/crates/lpe-core/src/sieve/context.md)

# Called by

- [run_update_scenarios](../../../../../functions/crates/lpe-storage/tests/outlook_cache_fidelity_update/run_update_scenarios.md)
---
type: Rust Function
title: execute_update_expect_failure
resource: crates/lpe-storage/tests/outlook_cache_fidelity_update.rs#L226-L240
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/context
  - functions/crates/lpe-activesync/src/tests/query
  called_by:
  - functions/crates/lpe-storage/tests/outlook_cache_fidelity_update/run_update_scenarios
---

# Signature

`async fn execute_update_expect_failure(pool: &PgPool, update_sql: &str) -> Result<String>`

# Calls

- [context](../../../../../functions/crates/lpe-core/src/sieve/context.md)
- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)

# Called by

- [run_update_scenarios](../../../../../functions/crates/lpe-storage/tests/outlook_cache_fidelity_update/run_update_scenarios.md)
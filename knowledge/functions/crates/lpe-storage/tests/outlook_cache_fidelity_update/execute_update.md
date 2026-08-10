---
type: Rust Function
title: execute_update
resource: crates/lpe-storage/tests/outlook_cache_fidelity_update.rs#L212-L224
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/context
  - functions/tools/rca_outlook_connectivity_check/execute
  - functions/crates/lpe-activesync/src/tests/query
---

# Signature

`async fn execute_update(pool: &PgPool, update_sql: &str) -> Result<()>`

# Calls

- [context](../../../../../functions/crates/lpe-core/src/sieve/context.md)
- [execute](../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)
- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
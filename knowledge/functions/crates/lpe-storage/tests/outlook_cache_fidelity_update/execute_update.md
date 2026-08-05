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
  - functions/crates/lpe-activesync/src/tests/query
---

# Signature

`async fn execute_update(pool: &PgPool, update_sql: &str) -> Result<()>`

# Calls

- [context](../../../../../functions/crates/lpe-core/src/sieve/context.md)
- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
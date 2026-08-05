---
type: Rust Function
title: logical_index_is_unique
resource: crates/lpe-storage/tests/outlook_cache_fidelity_update.rs#L389-L404
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/context
---

# Signature

`async fn logical_index_is_unique(pool: &PgPool, schema_name: &str) -> Result<bool>`

# Calls

- [context](../../../../../functions/crates/lpe-core/src/sieve/context.md)
---
type: Rust Function
title: assert_schema_version
resource: crates/lpe-storage/tests/schema_051_update.rs#L322-L344
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-core/src/sieve/context
---

# Signature

`async fn assert_schema_version(pool: &PgPool, schema_name: &str, expected: &str) -> Result<()>`

# Calls

- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [context](../../../../../functions/crates/lpe-core/src/sieve/context.md)
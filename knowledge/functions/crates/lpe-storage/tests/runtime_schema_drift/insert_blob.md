---
type: Rust Function
title: insert_blob
resource: crates/lpe-storage/tests/runtime_schema_drift.rs#L629-L654
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/tests/runtime_schema_drift/hex64
---

# Signature

`async fn insert_blob( pool: &PgPool, tenant_id: Uuid, domain_id: Uuid, blob_id: Uuid, blob_kind: &str, salt: u8, ) -> Result<()>`

# Calls

- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [hex64](../../../../../functions/crates/lpe-storage/tests/runtime_schema_drift/hex64.md)
---
type: Rust Function
title: insert_blob
resource: crates/lpe-storage/src/storage_visibility/tests.rs#L54-L76
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/core/Storage/pool
  - functions/crates/lpe-core/src/sieve/Parser/expect
---

# Signature

`async fn insert_blob(storage: &Storage, tenant_id: Uuid, domain_id: Uuid) -> Uuid`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [pool](../../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
---
type: Rust Function
title: database_blob_bytes_len
resource: crates/lpe-storage/src/blob_store/tests.rs#L442-L455
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/core/Storage/pool
  - functions/crates/lpe-core/src/sieve/Parser/expect
---

# Signature

`async fn database_blob_bytes_len(storage: &Storage, tenant_id: Uuid, blob_id: Uuid) -> Option<i64>`

# Calls

- [pool](../../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
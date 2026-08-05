---
type: Rust Function
title: active_storage_pool_id
resource: crates/lpe-storage/src/blob_store/tests.rs#L424-L440
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/core/Storage/pool
  - functions/crates/lpe-core/src/sieve/Parser/expect
---

# Signature

`async fn active_storage_pool_id(storage: &Storage, tenant_id: Uuid, blob_id: Uuid) -> Uuid`

# Calls

- [pool](../../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
---
type: Rust Function
title: placement_count_for_pool
resource: crates/lpe-storage/src/blob_store/tests.rs#L401-L422
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/core/Storage/pool
  - functions/crates/lpe-core/src/sieve/Parser/expect
---

# Signature

`async fn placement_count_for_pool( storage: &Storage, tenant_id: Uuid, blob_id: Uuid, storage_pool_id: Uuid, ) -> i64`

# Calls

- [pool](../../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
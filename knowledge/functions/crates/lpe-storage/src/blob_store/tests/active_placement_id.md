---
type: Rust Function
title: active_placement_id
resource: crates/lpe-storage/src/blob_store/tests.rs#L345-L361
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/core/Storage/pool
  - functions/crates/lpe-core/src/sieve/Parser/expect
  called_by:
  - functions/crates/lpe-storage/src/blob_store/tests/cleanup_worker_refuses_the_only_active_placement
---

# Signature

`async fn active_placement_id(storage: &Storage, tenant_id: Uuid, blob_id: Uuid) -> Uuid`

# Calls

- [pool](../../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)

# Called by

- [cleanup_worker_refuses_the_only_active_placement](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/cleanup_worker_refuses_the_only_active_placement.md)
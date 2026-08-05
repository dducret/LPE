---
type: Rust Function
title: placement_status_by_id
resource: crates/lpe-storage/src/blob_store/tests.rs#L331-L343
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/core/Storage/pool
  - functions/crates/lpe-core/src/sieve/Parser/expect
---

# Signature

`async fn placement_status_by_id(storage: &Storage, placement_id: Uuid) -> String`

# Calls

- [pool](../../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
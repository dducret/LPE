---
type: Rust Function
title: configure_s3_platform_pool
resource: crates/lpe-storage/src/blob_store/tests.rs#L106-L120
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/blob_store/tests/insert_s3_storage_pool
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/core/Storage/pool
  - functions/crates/lpe-core/src/sieve/Parser/expect
  called_by:
  - functions/crates/lpe-storage/src/blob_store/tests/s3_compatible_backend_put_read_stat_and_verify_round_trip
---

# Signature

`async fn configure_s3_platform_pool(storage: &Storage, config: Value) -> Uuid`

# Calls

- [insert_s3_storage_pool](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/insert_s3_storage_pool.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [pool](../../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)

# Called by

- [s3_compatible_backend_put_read_stat_and_verify_round_trip](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/s3_compatible_backend_put_read_stat_and_verify_round_trip.md)
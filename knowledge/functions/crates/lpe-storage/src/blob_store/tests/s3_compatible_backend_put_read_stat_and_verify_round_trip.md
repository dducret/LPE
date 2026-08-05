---
type: Rust Function
title: s3_compatible_backend_put_read_stat_and_verify_round_trip
resource: crates/lpe-storage/src/blob_store/tests.rs#L2261-L2346
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/blob_store/tests/configure_s3_platform_pool
  - functions/crates/lpe-storage/src/core/Storage/pool
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/put_durable_blob_in_tx
  - functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/read_durable_blob
  - functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/stat_durable_blob
---

# Signature

`async fn s3_compatible_backend_put_read_stat_and_verify_round_trip()`

# Calls

- [configure_s3_platform_pool](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/configure_s3_platform_pool.md)
- [pool](../../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [put_durable_blob_in_tx](../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/put_durable_blob_in_tx.md)
- [read_durable_blob](../../../../../../functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/read_durable_blob.md)
- [stat_durable_blob](../../../../../../functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/stat_durable_blob.md)
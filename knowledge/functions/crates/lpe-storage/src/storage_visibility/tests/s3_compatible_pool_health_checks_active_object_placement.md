---
type: Rust Function
title: s3_compatible_pool_health_checks_active_object_placement
resource: crates/lpe-storage/src/storage_visibility/tests.rs#L364-L457
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/core/Storage/pool
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-domain/src/crypto/sha256_hex
  - functions/crates/lpe-storage/src/storage_backend/select_storage_backend
  - functions/crates/lpe-storage/src/storage_backend/s3_put_object
  - functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_platform_storage_health
---

# Signature

`async fn s3_compatible_pool_health_checks_active_object_placement()`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [pool](../../../../../../functions/crates/lpe-storage/src/core/Storage/pool.md)
- [expect](../../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [sha256_hex](../../../../../../functions/crates/lpe-domain/src/crypto/sha256_hex.md)
- [select_storage_backend](../../../../../../functions/crates/lpe-storage/src/storage_backend/select_storage_backend.md)
- [s3_put_object](../../../../../../functions/crates/lpe-storage/src/storage_backend/s3_put_object.md)
- [fetch_platform_storage_health](../../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/fetch_platform_storage_health.md)
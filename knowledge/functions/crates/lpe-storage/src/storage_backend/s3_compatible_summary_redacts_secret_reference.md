---
type: Rust Function
title: s3_compatible_summary_redacts_secret_reference
resource: crates/lpe-storage/src/storage_backend.rs#L876-L897
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/storage_backend/normalize_storage_pool_config
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-storage/src/storage_backend/storage_pool_config_summary
---

# Signature

`fn s3_compatible_summary_redacts_secret_reference()`

# Calls

- [normalize_storage_pool_config](../../../../../functions/crates/lpe-storage/src/storage_backend/normalize_storage_pool_config.md)
- [expect](../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [storage_pool_config_summary](../../../../../functions/crates/lpe-storage/src/storage_backend/storage_pool_config_summary.md)
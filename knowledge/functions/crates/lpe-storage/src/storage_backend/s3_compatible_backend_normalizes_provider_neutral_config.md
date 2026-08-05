---
type: Rust Function
title: s3_compatible_backend_normalizes_provider_neutral_config
resource: crates/lpe-storage/src/storage_backend.rs#L829-L855
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/storage_backend/normalize_storage_pool_config
  - functions/crates/lpe-core/src/sieve/Parser/expect
  - functions/crates/lpe-storage/src/storage_backend/select_storage_backend
---

# Signature

`fn s3_compatible_backend_normalizes_provider_neutral_config()`

# Calls

- [normalize_storage_pool_config](../../../../../functions/crates/lpe-storage/src/storage_backend/normalize_storage_pool_config.md)
- [expect](../../../../../functions/crates/lpe-core/src/sieve/Parser/expect.md)
- [select_storage_backend](../../../../../functions/crates/lpe-storage/src/storage_backend/select_storage_backend.md)
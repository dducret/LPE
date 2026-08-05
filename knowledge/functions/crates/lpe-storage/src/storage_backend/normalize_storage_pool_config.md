---
type: Rust Function
title: normalize_storage_pool_config
resource: crates/lpe-storage/src/storage_backend.rs#L108-L117
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/storage_backend/normalize_storage_pool_kind
  - functions/crates/lpe-storage/src/storage_backend/normalize_postgres_config
  - functions/crates/lpe-storage/src/storage_backend/normalize_s3_compatible_config
  called_by:
  - functions/crates/lpe-storage/src/storage_backend/postgres_backend_accepts_empty_config_only
  - functions/crates/lpe-storage/src/storage_backend/s3_compatible_backend_normalizes_provider_neutral_config
  - functions/crates/lpe-storage/src/storage_backend/s3_compatible_backend_rejects_inline_credentials
  - functions/crates/lpe-storage/src/storage_backend/s3_compatible_summary_redacts_secret_reference
  - functions/crates/lpe-storage/src/storage_policy/Storage/create_storage_pool
  - functions/crates/lpe-storage/src/storage_policy/Storage/update_storage_pool
---

# Signature

`pub(crate) fn normalize_storage_pool_config( pool_kind: &str, config: Option<Value>, ) -> Result<Value>`

# Calls

- [normalize_storage_pool_kind](../../../../../functions/crates/lpe-storage/src/storage_backend/normalize_storage_pool_kind.md)
- [normalize_postgres_config](../../../../../functions/crates/lpe-storage/src/storage_backend/normalize_postgres_config.md)
- [normalize_s3_compatible_config](../../../../../functions/crates/lpe-storage/src/storage_backend/normalize_s3_compatible_config.md)

# Called by

- [postgres_backend_accepts_empty_config_only](../../../../../functions/crates/lpe-storage/src/storage_backend/postgres_backend_accepts_empty_config_only.md)
- [s3_compatible_backend_normalizes_provider_neutral_config](../../../../../functions/crates/lpe-storage/src/storage_backend/s3_compatible_backend_normalizes_provider_neutral_config.md)
- [s3_compatible_backend_rejects_inline_credentials](../../../../../functions/crates/lpe-storage/src/storage_backend/s3_compatible_backend_rejects_inline_credentials.md)
- [s3_compatible_summary_redacts_secret_reference](../../../../../functions/crates/lpe-storage/src/storage_backend/s3_compatible_summary_redacts_secret_reference.md)
- [create_storage_pool](../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/create_storage_pool.md)
- [update_storage_pool](../../../../../functions/crates/lpe-storage/src/storage_policy/Storage/update_storage_pool.md)
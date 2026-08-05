---
type: Rust Function
title: storage_pool_config_summary
resource: crates/lpe-storage/src/storage_backend.rs#L135-L150
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/storage_backend/select_storage_backend
  called_by:
  - functions/crates/lpe-storage/src/storage_backend/s3_compatible_summary_redacts_secret_reference
  - functions/crates/lpe-storage/src/storage_policy/storage_pool_summary
---

# Signature

`pub(crate) fn storage_pool_config_summary( pool_kind: &str, config: &Value, ) -> Result<Option<StoragePoolConfigSummary>>`

# Calls

- [select_storage_backend](../../../../../functions/crates/lpe-storage/src/storage_backend/select_storage_backend.md)

# Called by

- [s3_compatible_summary_redacts_secret_reference](../../../../../functions/crates/lpe-storage/src/storage_backend/s3_compatible_summary_redacts_secret_reference.md)
- [storage_pool_summary](../../../../../functions/crates/lpe-storage/src/storage_policy/storage_pool_summary.md)
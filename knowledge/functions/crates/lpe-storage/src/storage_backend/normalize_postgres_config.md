---
type: Rust Function
title: normalize_postgres_config
resource: crates/lpe-storage/src/storage_backend.rs#L312-L318
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/storage_backend/normalize_storage_pool_config
  - functions/crates/lpe-storage/src/storage_backend/select_storage_backend
---

# Signature

`fn normalize_postgres_config(config: Option<Value>) -> Result<Value>`

# Called by

- [normalize_storage_pool_config](../../../../../functions/crates/lpe-storage/src/storage_backend/normalize_storage_pool_config.md)
- [select_storage_backend](../../../../../functions/crates/lpe-storage/src/storage_backend/select_storage_backend.md)
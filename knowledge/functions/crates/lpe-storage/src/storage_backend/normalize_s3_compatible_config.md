---
type: Rust Function
title: normalize_s3_compatible_config
resource: crates/lpe-storage/src/storage_backend.rs#L320-L335
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/storage_backend/parse_s3_compatible_config_from_map
  called_by:
  - functions/crates/lpe-storage/src/storage_backend/normalize_storage_pool_config
---

# Signature

`fn normalize_s3_compatible_config(config: Option<Value>) -> Result<Value>`

# Calls

- [parse_s3_compatible_config_from_map](../../../../../functions/crates/lpe-storage/src/storage_backend/parse_s3_compatible_config_from_map.md)

# Called by

- [normalize_storage_pool_config](../../../../../functions/crates/lpe-storage/src/storage_backend/normalize_storage_pool_config.md)
---
type: Rust Function
title: parse_s3_compatible_config
resource: crates/lpe-storage/src/storage_backend.rs#L337-L342
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/storage_backend/parse_s3_compatible_config_from_map
  called_by:
  - functions/crates/lpe-storage/src/storage_backend/select_storage_backend
---

# Signature

`fn parse_s3_compatible_config(config: &Value) -> Result<S3CompatiblePoolConfig>`

# Calls

- [parse_s3_compatible_config_from_map](../../../../../functions/crates/lpe-storage/src/storage_backend/parse_s3_compatible_config_from_map.md)

# Called by

- [select_storage_backend](../../../../../functions/crates/lpe-storage/src/storage_backend/select_storage_backend.md)
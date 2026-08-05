---
type: Rust Function
title: normalize_addressing_style
resource: crates/lpe-storage/src/storage_backend.rs#L461-L469
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/storage_backend/parse_s3_compatible_config_from_map
---

# Signature

`fn normalize_addressing_style(value: &str) -> Result<S3AddressingStyle>`

# Called by

- [parse_s3_compatible_config_from_map](../../../../../functions/crates/lpe-storage/src/storage_backend/parse_s3_compatible_config_from_map.md)
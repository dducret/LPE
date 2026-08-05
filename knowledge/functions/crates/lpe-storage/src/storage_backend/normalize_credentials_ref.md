---
type: Rust Function
title: normalize_credentials_ref
resource: crates/lpe-storage/src/storage_backend.rs#L787-L796
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/storage_backend/has_control_or_whitespace
  called_by:
  - functions/crates/lpe-storage/src/storage_backend/parse_s3_compatible_config_from_map
---

# Signature

`fn normalize_credentials_ref(value: &str) -> Result<String>`

# Calls

- [has_control_or_whitespace](../../../../../functions/crates/lpe-storage/src/storage_backend/has_control_or_whitespace.md)

# Called by

- [parse_s3_compatible_config_from_map](../../../../../functions/crates/lpe-storage/src/storage_backend/parse_s3_compatible_config_from_map.md)
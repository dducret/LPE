---
type: Rust Function
title: required_region
resource: crates/lpe-storage/src/storage_backend.rs#L418-L428
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/storage_backend/optional_string
  called_by:
  - functions/crates/lpe-storage/src/storage_backend/parse_s3_compatible_config_from_map
---

# Signature

`fn required_region(config: &Map<String, Value>) -> Result<&str>`

# Calls

- [optional_string](../../../../../functions/crates/lpe-storage/src/storage_backend/optional_string.md)

# Called by

- [parse_s3_compatible_config_from_map](../../../../../functions/crates/lpe-storage/src/storage_backend/parse_s3_compatible_config_from_map.md)
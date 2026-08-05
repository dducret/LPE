---
type: Rust Function
title: normalize_endpoint_url
resource: crates/lpe-storage/src/storage_backend.rs#L430-L445
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/storage_backend/has_control_or_whitespace
  called_by:
  - functions/crates/lpe-storage/src/storage_backend/parse_s3_compatible_config_from_map
---

# Signature

`fn normalize_endpoint_url(value: &str) -> Result<String>`

# Calls

- [query](../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [has_control_or_whitespace](../../../../../functions/crates/lpe-storage/src/storage_backend/has_control_or_whitespace.md)

# Called by

- [parse_s3_compatible_config_from_map](../../../../../functions/crates/lpe-storage/src/storage_backend/parse_s3_compatible_config_from_map.md)
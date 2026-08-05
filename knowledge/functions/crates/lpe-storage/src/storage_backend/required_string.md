---
type: Rust Function
title: required_string
resource: crates/lpe-storage/src/storage_backend.rs#L394-L401
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-storage/src/storage_backend/parse_s3_compatible_config_from_map
---

# Signature

`fn required_string<'a>(config: &'a Map<String, Value>, key: &str) -> Result<&'a str>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [parse_s3_compatible_config_from_map](../../../../../functions/crates/lpe-storage/src/storage_backend/parse_s3_compatible_config_from_map.md)
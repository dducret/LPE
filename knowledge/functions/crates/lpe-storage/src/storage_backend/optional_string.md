---
type: Rust Function
title: optional_string
resource: crates/lpe-storage/src/storage_backend.rs#L403-L416
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-storage/src/storage_backend/parse_s3_compatible_config_from_map
  - functions/crates/lpe-storage/src/storage_backend/required_region
---

# Signature

`fn optional_string<'a>(config: &'a Map<String, Value>, key: &str) -> Result<Option<&'a str>>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [parse_s3_compatible_config_from_map](../../../../../functions/crates/lpe-storage/src/storage_backend/parse_s3_compatible_config_from_map.md)
- [required_region](../../../../../functions/crates/lpe-storage/src/storage_backend/required_region.md)
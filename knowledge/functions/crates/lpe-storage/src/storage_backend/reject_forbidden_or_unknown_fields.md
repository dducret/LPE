---
type: Rust Function
title: reject_forbidden_or_unknown_fields
resource: crates/lpe-storage/src/storage_backend.rs#L366-L392
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/storage_backend/parse_s3_compatible_config_from_map
---

# Signature

`fn reject_forbidden_or_unknown_fields(config: &Map<String, Value>) -> Result<()>`

# Called by

- [parse_s3_compatible_config_from_map](../../../../../functions/crates/lpe-storage/src/storage_backend/parse_s3_compatible_config_from_map.md)
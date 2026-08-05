---
type: Rust Function
title: parse_s3_compatible_config_from_map
resource: crates/lpe-storage/src/storage_backend.rs#L344-L364
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/storage_backend/reject_forbidden_or_unknown_fields
  - functions/crates/lpe-storage/src/storage_backend/normalize_endpoint_url
  - functions/crates/lpe-storage/src/storage_backend/required_string
  - functions/crates/lpe-storage/src/storage_backend/normalize_bucket
  - functions/crates/lpe-storage/src/storage_backend/normalize_signing_region
  - functions/crates/lpe-storage/src/storage_backend/required_region
  - functions/crates/lpe-storage/src/storage_backend/normalize_addressing_style
  - functions/crates/lpe-storage/src/storage_backend/optional_string
  - functions/crates/lpe-storage/src/storage_backend/normalize_credentials_ref
  called_by:
  - functions/crates/lpe-storage/src/storage_backend/normalize_s3_compatible_config
  - functions/crates/lpe-storage/src/storage_backend/parse_s3_compatible_config
---

# Signature

`fn parse_s3_compatible_config_from_map( config: &Map<String, Value>, ) -> Result<S3CompatiblePoolConfig>`

# Calls

- [reject_forbidden_or_unknown_fields](../../../../../functions/crates/lpe-storage/src/storage_backend/reject_forbidden_or_unknown_fields.md)
- [normalize_endpoint_url](../../../../../functions/crates/lpe-storage/src/storage_backend/normalize_endpoint_url.md)
- [required_string](../../../../../functions/crates/lpe-storage/src/storage_backend/required_string.md)
- [normalize_bucket](../../../../../functions/crates/lpe-storage/src/storage_backend/normalize_bucket.md)
- [normalize_signing_region](../../../../../functions/crates/lpe-storage/src/storage_backend/normalize_signing_region.md)
- [required_region](../../../../../functions/crates/lpe-storage/src/storage_backend/required_region.md)
- [normalize_addressing_style](../../../../../functions/crates/lpe-storage/src/storage_backend/normalize_addressing_style.md)
- [optional_string](../../../../../functions/crates/lpe-storage/src/storage_backend/optional_string.md)
- [normalize_credentials_ref](../../../../../functions/crates/lpe-storage/src/storage_backend/normalize_credentials_ref.md)

# Called by

- [normalize_s3_compatible_config](../../../../../functions/crates/lpe-storage/src/storage_backend/normalize_s3_compatible_config.md)
- [parse_s3_compatible_config](../../../../../functions/crates/lpe-storage/src/storage_backend/parse_s3_compatible_config.md)
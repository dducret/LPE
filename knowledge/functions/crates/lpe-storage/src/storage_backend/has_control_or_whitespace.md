---
type: Rust Function
title: has_control_or_whitespace
resource: crates/lpe-storage/src/storage_backend.rs#L798-L802
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/storage_backend/normalize_endpoint_url
  - functions/crates/lpe-storage/src/storage_backend/normalize_bucket
  - functions/crates/lpe-storage/src/storage_backend/normalize_signing_region
  - functions/crates/lpe-storage/src/storage_backend/normalize_object_prefix
  - functions/crates/lpe-storage/src/storage_backend/normalize_credentials_ref
---

# Signature

`fn has_control_or_whitespace(value: &str) -> bool`

# Called by

- [normalize_endpoint_url](../../../../../functions/crates/lpe-storage/src/storage_backend/normalize_endpoint_url.md)
- [normalize_bucket](../../../../../functions/crates/lpe-storage/src/storage_backend/normalize_bucket.md)
- [normalize_signing_region](../../../../../functions/crates/lpe-storage/src/storage_backend/normalize_signing_region.md)
- [normalize_object_prefix](../../../../../functions/crates/lpe-storage/src/storage_backend/normalize_object_prefix.md)
- [normalize_credentials_ref](../../../../../functions/crates/lpe-storage/src/storage_backend/normalize_credentials_ref.md)
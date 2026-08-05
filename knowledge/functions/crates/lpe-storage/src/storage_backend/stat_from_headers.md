---
type: Rust Function
title: stat_from_headers
resource: crates/lpe-storage/src/storage_backend.rs#L666-L691
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-storage/src/storage_backend/s3_stat_object
---

# Signature

`fn stat_from_headers(headers: &HeaderMap) -> Result<S3ObjectStat>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [s3_stat_object](../../../../../functions/crates/lpe-storage/src/storage_backend/s3_stat_object.md)
---
type: Rust Function
title: s3_object_url
resource: crates/lpe-storage/src/storage_backend.rs#L521-L557
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/storage_backend/percent_encode_path
  called_by:
  - functions/crates/lpe-storage/src/storage_backend/s3_put_object
  - functions/crates/lpe-storage/src/storage_backend/s3_read_object
  - functions/crates/lpe-storage/src/storage_backend/s3_stat_object
---

# Signature

`fn s3_object_url(config: &S3CompatiblePoolConfig, key: &str) -> Result<Url>`

# Calls

- [percent_encode_path](../../../../../functions/crates/lpe-storage/src/storage_backend/percent_encode_path.md)

# Called by

- [s3_put_object](../../../../../functions/crates/lpe-storage/src/storage_backend/s3_put_object.md)
- [s3_read_object](../../../../../functions/crates/lpe-storage/src/storage_backend/s3_read_object.md)
- [s3_stat_object](../../../../../functions/crates/lpe-storage/src/storage_backend/s3_stat_object.md)
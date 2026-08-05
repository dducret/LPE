---
type: Rust Function
title: s3_bucket_url
resource: crates/lpe-storage/src/storage_backend.rs#L559-L588
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/storage_backend/percent_encode_segment
  called_by:
  - functions/crates/lpe-storage/src/storage_backend/s3_probe_pool
---

# Signature

`fn s3_bucket_url(config: &S3CompatiblePoolConfig) -> Result<Url>`

# Calls

- [percent_encode_segment](../../../../../functions/crates/lpe-storage/src/storage_backend/percent_encode_segment.md)

# Called by

- [s3_probe_pool](../../../../../functions/crates/lpe-storage/src/storage_backend/s3_probe_pool.md)
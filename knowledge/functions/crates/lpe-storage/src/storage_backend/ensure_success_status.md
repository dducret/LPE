---
type: Rust Function
title: ensure_success_status
resource: crates/lpe-storage/src/storage_backend.rs#L693-L699
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/storage_backend/map_s3_status_error
  called_by:
  - functions/crates/lpe-storage/src/storage_backend/s3_put_object
  - functions/crates/lpe-storage/src/storage_backend/s3_read_object
  - functions/crates/lpe-storage/src/storage_backend/s3_stat_object
  - functions/crates/lpe-storage/src/storage_backend/s3_probe_pool
---

# Signature

`fn ensure_success_status(status: StatusCode, operation: &str) -> Result<()>`

# Calls

- [map_s3_status_error](../../../../../functions/crates/lpe-storage/src/storage_backend/map_s3_status_error.md)

# Called by

- [s3_put_object](../../../../../functions/crates/lpe-storage/src/storage_backend/s3_put_object.md)
- [s3_read_object](../../../../../functions/crates/lpe-storage/src/storage_backend/s3_read_object.md)
- [s3_stat_object](../../../../../functions/crates/lpe-storage/src/storage_backend/s3_stat_object.md)
- [s3_probe_pool](../../../../../functions/crates/lpe-storage/src/storage_backend/s3_probe_pool.md)
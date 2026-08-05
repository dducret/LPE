---
type: Rust Function
title: map_s3_status_error
resource: crates/lpe-storage/src/storage_backend.rs#L296-L310
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/storage_backend/ensure_success_status
  - functions/crates/lpe-storage/src/storage_backend/s3_status_errors_are_storage_backend_errors
---

# Signature

`pub(crate) fn map_s3_status_error(status: StatusCode, operation: &str) -> StorageBackendError`

# Called by

- [ensure_success_status](../../../../../functions/crates/lpe-storage/src/storage_backend/ensure_success_status.md)
- [s3_status_errors_are_storage_backend_errors](../../../../../functions/crates/lpe-storage/src/storage_backend/s3_status_errors_are_storage_backend_errors.md)
---
type: Rust Function
title: s3_probe_pool
resource: crates/lpe-storage/src/storage_backend.rs#L275-L294
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/storage_backend/resolve_s3_credentials
  - functions/crates/lpe-storage/src/storage_backend/s3_bucket_url
  - functions/crates/lpe-storage/src/storage_backend/signed_s3_request
  - functions/crates/lpe-storage/src/storage_backend/ensure_success_status
  - functions/LPE-CT/src/host_logs/HostLogError/status
  called_by:
  - functions/crates/lpe-storage/src/storage_visibility/Storage/check_pool_backend_health
---

# Signature

`pub(crate) async fn s3_probe_pool(config: &S3CompatiblePoolConfig) -> Result<()>`

# Calls

- [resolve_s3_credentials](../../../../../functions/crates/lpe-storage/src/storage_backend/resolve_s3_credentials.md)
- [s3_bucket_url](../../../../../functions/crates/lpe-storage/src/storage_backend/s3_bucket_url.md)
- [signed_s3_request](../../../../../functions/crates/lpe-storage/src/storage_backend/signed_s3_request.md)
- [ensure_success_status](../../../../../functions/crates/lpe-storage/src/storage_backend/ensure_success_status.md)
- [status](../../../../../functions/LPE-CT/src/host_logs/HostLogError/status.md)

# Called by

- [check_pool_backend_health](../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/check_pool_backend_health.md)
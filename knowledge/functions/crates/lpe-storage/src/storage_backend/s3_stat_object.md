---
type: Rust Function
title: s3_stat_object
resource: crates/lpe-storage/src/storage_backend.rs#L249-L273
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/storage_backend/s3_object_key_for_placement
  - functions/crates/lpe-storage/src/storage_backend/resolve_s3_credentials
  - functions/crates/lpe-storage/src/storage_backend/s3_object_url
  - functions/crates/lpe-storage/src/storage_backend/signed_s3_request
  - functions/crates/lpe-storage/src/storage_backend/ensure_success_status
  - functions/LPE-CT/src/host_logs/HostLogError/status
  - functions/crates/lpe-storage/src/storage_backend/stat_from_headers
  called_by:
  - functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/stat_durable_blob
  - functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/verify_durable_blob
  - functions/crates/lpe-storage/src/storage_backend/s3_put_object
  - functions/crates/lpe-storage/src/storage_visibility/Storage/check_pool_backend_health
---

# Signature

`pub(crate) async fn s3_stat_object( config: &S3CompatiblePoolConfig, placement_id: Uuid, ) -> Result<S3ObjectStat>`

# Calls

- [s3_object_key_for_placement](../../../../../functions/crates/lpe-storage/src/storage_backend/s3_object_key_for_placement.md)
- [resolve_s3_credentials](../../../../../functions/crates/lpe-storage/src/storage_backend/resolve_s3_credentials.md)
- [s3_object_url](../../../../../functions/crates/lpe-storage/src/storage_backend/s3_object_url.md)
- [signed_s3_request](../../../../../functions/crates/lpe-storage/src/storage_backend/signed_s3_request.md)
- [ensure_success_status](../../../../../functions/crates/lpe-storage/src/storage_backend/ensure_success_status.md)
- [status](../../../../../functions/LPE-CT/src/host_logs/HostLogError/status.md)
- [stat_from_headers](../../../../../functions/crates/lpe-storage/src/storage_backend/stat_from_headers.md)

# Called by

- [stat_durable_blob](../../../../../functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/stat_durable_blob.md)
- [verify_durable_blob](../../../../../functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/verify_durable_blob.md)
- [s3_put_object](../../../../../functions/crates/lpe-storage/src/storage_backend/s3_put_object.md)
- [check_pool_backend_health](../../../../../functions/crates/lpe-storage/src/storage_visibility/Storage/check_pool_backend_health.md)
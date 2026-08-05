---
type: Rust Function
title: s3_read_object
resource: crates/lpe-storage/src/storage_backend.rs#L222-L247
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/storage_backend/s3_object_key_for_placement
  - functions/crates/lpe-storage/src/storage_backend/resolve_s3_credentials
  - functions/crates/lpe-storage/src/storage_backend/s3_object_url
  - functions/crates/lpe-storage/src/storage_backend/signed_s3_request
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-storage/src/storage_backend/ensure_success_status
  - functions/LPE-CT/src/host_logs/HostLogError/status
  called_by:
  - functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/read_durable_blob
  - functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/read_placement_bytes
---

# Signature

`pub(crate) async fn s3_read_object( config: &S3CompatiblePoolConfig, placement_id: Uuid, ) -> Result<Vec<u8>>`

# Calls

- [s3_object_key_for_placement](../../../../../functions/crates/lpe-storage/src/storage_backend/s3_object_key_for_placement.md)
- [resolve_s3_credentials](../../../../../functions/crates/lpe-storage/src/storage_backend/resolve_s3_credentials.md)
- [s3_object_url](../../../../../functions/crates/lpe-storage/src/storage_backend/s3_object_url.md)
- [signed_s3_request](../../../../../functions/crates/lpe-storage/src/storage_backend/signed_s3_request.md)
- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [ensure_success_status](../../../../../functions/crates/lpe-storage/src/storage_backend/ensure_success_status.md)
- [status](../../../../../functions/LPE-CT/src/host_logs/HostLogError/status.md)

# Called by

- [read_durable_blob](../../../../../functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/read_durable_blob.md)
- [read_placement_bytes](../../../../../functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/read_placement_bytes.md)
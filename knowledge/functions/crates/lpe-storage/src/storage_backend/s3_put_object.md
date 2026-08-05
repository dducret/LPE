---
type: Rust Function
title: s3_put_object
resource: crates/lpe-storage/src/storage_backend.rs#L164-L220
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-domain/src/crypto/sha256_hex
  - functions/crates/lpe-storage/src/storage_backend/s3_object_key_for_placement
  - functions/crates/lpe-storage/src/storage_backend/resolve_s3_credentials
  - functions/crates/lpe-storage/src/storage_backend/s3_object_url
  - functions/crates/lpe-storage/src/storage_backend/signed_s3_request
  - functions/crates/lpe-storage/src/storage_backend/ensure_success_status
  - functions/LPE-CT/src/host_logs/HostLogError/status
  - functions/crates/lpe-storage/src/storage_backend/s3_stat_object
  called_by:
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/ensure_backend_placement_in_tx
  - functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/write_migration_target_placement
  - functions/crates/lpe-storage/src/storage_visibility/tests/s3_compatible_pool_health_checks_active_object_placement
---

# Signature

`pub(crate) async fn s3_put_object( config: &S3CompatiblePoolConfig, placement_id: Uuid, bytes: &[u8], expected_sha256: &str, expected_size_octets: i64, ) -> Result<S3ObjectStat>`

# Calls

- [sha256_hex](../../../../../functions/crates/lpe-domain/src/crypto/sha256_hex.md)
- [s3_object_key_for_placement](../../../../../functions/crates/lpe-storage/src/storage_backend/s3_object_key_for_placement.md)
- [resolve_s3_credentials](../../../../../functions/crates/lpe-storage/src/storage_backend/resolve_s3_credentials.md)
- [s3_object_url](../../../../../functions/crates/lpe-storage/src/storage_backend/s3_object_url.md)
- [signed_s3_request](../../../../../functions/crates/lpe-storage/src/storage_backend/signed_s3_request.md)
- [ensure_success_status](../../../../../functions/crates/lpe-storage/src/storage_backend/ensure_success_status.md)
- [status](../../../../../functions/LPE-CT/src/host_logs/HostLogError/status.md)
- [s3_stat_object](../../../../../functions/crates/lpe-storage/src/storage_backend/s3_stat_object.md)

# Called by

- [ensure_backend_placement_in_tx](../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/ensure_backend_placement_in_tx.md)
- [write_migration_target_placement](../../../../../functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/write_migration_target_placement.md)
- [s3_compatible_pool_health_checks_active_object_placement](../../../../../functions/crates/lpe-storage/src/storage_visibility/tests/s3_compatible_pool_health_checks_active_object_placement.md)
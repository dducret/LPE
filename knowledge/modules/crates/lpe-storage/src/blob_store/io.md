---
type: Rust Module
title: io
resource: crates/lpe-storage/src/blob_store/io.rs#L1-L396
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-result
  - external/sha2-digest-sha256
  - external/sqlx-pgpool-row
  - external/uuid-uuid
  - external/crate-storage-backend-s3-put-object-s3-read-object-s3-stat-object-select-storage-backend-storagebackendselection
  - external/super-activeblobplacement-blobmigrationjob-durableblobkind-migrationtargetplacement-postgresblobstore-storedblobbytes-storedblobstat
  member_of:
  - packages/crates/lpe-storage
---

# Contains

- [read_durable_blob](../../../../../functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/read_durable_blob.md)
- [stat_durable_blob](../../../../../functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/stat_durable_blob.md)
- [load_migration_source_placement](../../../../../functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/load_migration_source_placement.md)
- [read_placement_bytes](../../../../../functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/read_placement_bytes.md)
- [write_migration_target_placement](../../../../../functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/write_migration_target_placement.md)
- [load_active_blob_placement](../../../../../functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/load_active_blob_placement.md)
- [error_if_durable_blob_lacks_active_placement](../../../../../functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/error_if_durable_blob_lacks_active_placement.md)
- [verify_durable_blob](../../../../../functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/verify_durable_blob.md)

# Imports

- `anyhow::{anyhow, Result}`
- `sha2::{Digest, Sha256}`
- `sqlx::{PgPool, Row}`
- `uuid::Uuid`
- `crate::storage_backend::{
    s3_put_object, s3_read_object, s3_stat_object, select_storage_backend, StorageBackendSelection,
}`
- `super::{
    ActiveBlobPlacement, BlobMigrationJob, DurableBlobKind, MigrationTargetPlacement,
    PostgresBlobStore, StoredBlobBytes, StoredBlobStat,
}`

# Member of

- [lpe-storage](../../../../../packages/crates/lpe-storage.md)
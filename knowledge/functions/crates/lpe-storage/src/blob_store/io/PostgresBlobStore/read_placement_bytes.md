---
type: Rust Method
title: read_placement_bytes
resource: crates/lpe-storage/src/blob_store/io.rs#L162-L182
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/storage_backend/s3_read_object
  called_by:
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/copy_and_verify_one_blob_migration_job
---

# Signature

`pub(super) async fn read_placement_bytes( &self, placement: &ActiveBlobPlacement, ) -> Result<StoredBlobBytes>`

# Calls

- [s3_read_object](../../../../../../../functions/crates/lpe-storage/src/storage_backend/s3_read_object.md)

# Called by

- [copy_and_verify_one_blob_migration_job](../../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/copy_and_verify_one_blob_migration_job.md)
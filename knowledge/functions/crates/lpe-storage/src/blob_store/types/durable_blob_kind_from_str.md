---
type: Rust Function
title: durable_blob_kind_from_str
resource: crates/lpe-storage/src/blob_store/types.rs#L142-L148
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/blob_store/types/normalize_migration_blob_kind
  called_by:
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/copy_and_verify_one_blob_migration_job
---

# Signature

`pub(super) fn durable_blob_kind_from_str(blob_kind: &str) -> Result<DurableBlobKind>`

# Calls

- [normalize_migration_blob_kind](../../../../../../functions/crates/lpe-storage/src/blob_store/types/normalize_migration_blob_kind.md)

# Called by

- [copy_and_verify_one_blob_migration_job](../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/copy_and_verify_one_blob_migration_job.md)
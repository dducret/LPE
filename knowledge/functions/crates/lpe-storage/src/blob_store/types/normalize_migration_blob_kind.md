---
type: Rust Function
title: normalize_migration_blob_kind
resource: crates/lpe-storage/src/blob_store/types.rs#L131-L139
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/create_blob_migration_job
  - functions/crates/lpe-storage/src/blob_store/types/durable_blob_kind_from_str
---

# Signature

`pub(super) fn normalize_migration_blob_kind(blob_kind: &str) -> Result<&'static str>`

# Called by

- [create_blob_migration_job](../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/create_blob_migration_job.md)
- [durable_blob_kind_from_str](../../../../../../functions/crates/lpe-storage/src/blob_store/types/durable_blob_kind_from_str.md)
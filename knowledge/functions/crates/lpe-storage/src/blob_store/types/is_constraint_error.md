---
type: Rust Function
title: is_constraint_error
resource: crates/lpe-storage/src/blob_store/types.rs#L168-L174
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/create_blob_migration_job
---

# Signature

`pub(super) fn is_constraint_error(error: &sqlx::Error, constraint: &str) -> bool`

# Called by

- [create_blob_migration_job](../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/create_blob_migration_job.md)
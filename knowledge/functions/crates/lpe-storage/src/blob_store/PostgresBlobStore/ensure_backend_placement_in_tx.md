---
type: Rust Method
title: ensure_backend_placement_in_tx
resource: crates/lpe-storage/src/blob_store.rs#L165-L222
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/active_blob_placement_exists_in_tx
  - functions/crates/lpe-storage/src/storage_backend/s3_put_object
  - functions/crates/lpe-activesync/src/tests/query
  called_by:
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/put_durable_blob_in_tx
---

# Signature

`async fn ensure_backend_placement_in_tx( &self, tx: &mut sqlx::Transaction<'_, Postgres>, request: &PutBlobRequest<'_>, blob_id: Uuid, content_sha256: &str, size_octets: i64, write_pool: &WriteStoragePool, ) -> Result<()>`

# Calls

- [active_blob_placement_exists_in_tx](../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/active_blob_placement_exists_in_tx.md)
- [s3_put_object](../../../../../../functions/crates/lpe-storage/src/storage_backend/s3_put_object.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)

# Called by

- [put_durable_blob_in_tx](../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/put_durable_blob_in_tx.md)
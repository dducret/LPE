---
type: Rust Method
title: effective_write_storage_pool_in_tx
resource: crates/lpe-storage/src/blob_store.rs#L121-L163
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/storage_backend/select_storage_backend
  called_by:
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/put_durable_blob_in_tx
---

# Signature

`async fn effective_write_storage_pool_in_tx( &self, tx: &mut sqlx::Transaction<'_, Postgres>, request: &PutBlobRequest<'_>, ) -> Result<WriteStoragePool>`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [select_storage_backend](../../../../../../functions/crates/lpe-storage/src/storage_backend/select_storage_backend.md)

# Called by

- [put_durable_blob_in_tx](../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/put_durable_blob_in_tx.md)
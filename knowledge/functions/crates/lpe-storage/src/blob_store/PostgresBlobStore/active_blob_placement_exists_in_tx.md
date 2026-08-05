---
type: Rust Method
title: active_blob_placement_exists_in_tx
resource: crates/lpe-storage/src/blob_store.rs#L224-L250
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/ensure_backend_placement_in_tx
---

# Signature

`async fn active_blob_placement_exists_in_tx( &self, tx: &mut sqlx::Transaction<'_, Postgres>, request: &PutBlobRequest<'_>, blob_id: Uuid, ) -> Result<bool>`

# Called by

- [ensure_backend_placement_in_tx](../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/ensure_backend_placement_in_tx.md)
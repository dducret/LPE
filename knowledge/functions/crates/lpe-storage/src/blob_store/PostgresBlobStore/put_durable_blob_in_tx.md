---
type: Rust Method
title: put_durable_blob_in_tx
resource: crates/lpe-storage/src/blob_store.rs#L26-L119
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-domain/src/crypto/sha256_hex
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/effective_write_storage_pool_in_tx
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/ensure_backend_placement_in_tx
  called_by:
  - functions/crates/lpe-storage/src/attachments/Storage/store_attachment_blob_in_tx
  - functions/crates/lpe-storage/src/blob_store/tests/put_test_blob
  - functions/crates/lpe-storage/src/blob_store/tests/durable_blob_store_writes_reads_stats_and_verifies
  - functions/crates/lpe-storage/src/blob_store/tests/s3_compatible_backend_put_read_stat_and_verify_round_trip
---

# Signature

`pub(crate) async fn put_durable_blob_in_tx( &self, tx: &mut sqlx::Transaction<'_, Postgres>, request: PutBlobRequest<'_>, ) -> Result<StoredBlobRef>`

# Calls

- [sha256_hex](../../../../../../functions/crates/lpe-domain/src/crypto/sha256_hex.md)
- [effective_write_storage_pool_in_tx](../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/effective_write_storage_pool_in_tx.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [ensure_backend_placement_in_tx](../../../../../../functions/crates/lpe-storage/src/blob_store/PostgresBlobStore/ensure_backend_placement_in_tx.md)

# Called by

- [store_attachment_blob_in_tx](../../../../../../functions/crates/lpe-storage/src/attachments/Storage/store_attachment_blob_in_tx.md)
- [put_test_blob](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/put_test_blob.md)
- [durable_blob_store_writes_reads_stats_and_verifies](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/durable_blob_store_writes_reads_stats_and_verifies.md)
- [s3_compatible_backend_put_read_stat_and_verify_round_trip](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/s3_compatible_backend_put_read_stat_and_verify_round_trip.md)
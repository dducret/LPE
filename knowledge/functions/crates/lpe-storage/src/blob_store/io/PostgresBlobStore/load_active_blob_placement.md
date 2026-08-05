---
type: Rust Method
title: load_active_blob_placement
resource: crates/lpe-storage/src/blob_store/io.rs#L265-L324
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/storage_backend/select_storage_backend
  called_by:
  - functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/read_durable_blob
  - functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/stat_durable_blob
  - functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/verify_durable_blob
---

# Signature

`async fn load_active_blob_placement( &self, pool: &PgPool, tenant_id: &Uuid, domain_id: Uuid, kind: DurableBlobKind, blob_id: Uuid, ) -> Result<Option<ActiveBlobPlacement>>`

# Calls

- [query](../../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [select_storage_backend](../../../../../../../functions/crates/lpe-storage/src/storage_backend/select_storage_backend.md)

# Called by

- [read_durable_blob](../../../../../../../functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/read_durable_blob.md)
- [stat_durable_blob](../../../../../../../functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/stat_durable_blob.md)
- [verify_durable_blob](../../../../../../../functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/verify_durable_blob.md)
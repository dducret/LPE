---
type: Rust Method
title: error_if_durable_blob_lacks_active_placement
resource: crates/lpe-storage/src/blob_store/io.rs#L326-L359
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/read_durable_blob
  - functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/stat_durable_blob
  - functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/verify_durable_blob
---

# Signature

`async fn error_if_durable_blob_lacks_active_placement( &self, pool: &PgPool, tenant_id: &Uuid, domain_id: Uuid, kind: DurableBlobKind, blob_id: Uuid, ) -> Result<()>`

# Called by

- [read_durable_blob](../../../../../../../functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/read_durable_blob.md)
- [stat_durable_blob](../../../../../../../functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/stat_durable_blob.md)
- [verify_durable_blob](../../../../../../../functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/verify_durable_blob.md)
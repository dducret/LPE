---
type: Rust Method
title: verify_durable_blob
resource: crates/lpe-storage/src/blob_store/io.rs#L362-L395
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/load_active_blob_placement
  - functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/error_if_durable_blob_lacks_active_placement
  - functions/crates/lpe-storage/src/storage_backend/s3_stat_object
  called_by:
  - functions/crates/lpe-storage/src/blob_store/tests/durable_blob_store_writes_reads_stats_and_verifies
---

# Signature

`pub(crate) async fn verify_durable_blob( &self, pool: &PgPool, tenant_id: &Uuid, domain_id: Uuid, kind: DurableBlobKind, blob_id: Uuid, ) -> Result<bool>`

# Calls

- [load_active_blob_placement](../../../../../../../functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/load_active_blob_placement.md)
- [error_if_durable_blob_lacks_active_placement](../../../../../../../functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/error_if_durable_blob_lacks_active_placement.md)
- [s3_stat_object](../../../../../../../functions/crates/lpe-storage/src/storage_backend/s3_stat_object.md)

# Called by

- [durable_blob_store_writes_reads_stats_and_verifies](../../../../../../../functions/crates/lpe-storage/src/blob_store/tests/durable_blob_store_writes_reads_stats_and_verifies.md)
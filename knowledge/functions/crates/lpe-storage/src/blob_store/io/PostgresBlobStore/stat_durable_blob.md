---
type: Rust Method
title: stat_durable_blob
resource: crates/lpe-storage/src/blob_store/io.rs#L62-L97
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/load_active_blob_placement
  - functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/error_if_durable_blob_lacks_active_placement
  - functions/crates/lpe-storage/src/storage_backend/s3_stat_object
  called_by:
  - functions/crates/lpe-storage/src/activesync/Storage/fetch_activesync_message_attachments
  - functions/crates/lpe-storage/src/blob_store/tests/switch_preserves_reads_stats_and_verification_across_phases
  - functions/crates/lpe-storage/src/blob_store/tests/durable_blob_store_writes_reads_stats_and_verifies
  - functions/crates/lpe-storage/src/blob_store/tests/s3_compatible_backend_put_read_stat_and_verify_round_trip
---

# Signature

`pub(crate) async fn stat_durable_blob( &self, pool: &PgPool, tenant_id: &Uuid, domain_id: Uuid, kind: DurableBlobKind, blob_id: Uuid, ) -> Result<Option<StoredBlobStat>>`

# Calls

- [load_active_blob_placement](../../../../../../../functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/load_active_blob_placement.md)
- [error_if_durable_blob_lacks_active_placement](../../../../../../../functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/error_if_durable_blob_lacks_active_placement.md)
- [s3_stat_object](../../../../../../../functions/crates/lpe-storage/src/storage_backend/s3_stat_object.md)

# Called by

- [fetch_activesync_message_attachments](../../../../../../../functions/crates/lpe-storage/src/activesync/Storage/fetch_activesync_message_attachments.md)
- [switch_preserves_reads_stats_and_verification_across_phases](../../../../../../../functions/crates/lpe-storage/src/blob_store/tests/switch_preserves_reads_stats_and_verification_across_phases.md)
- [durable_blob_store_writes_reads_stats_and_verifies](../../../../../../../functions/crates/lpe-storage/src/blob_store/tests/durable_blob_store_writes_reads_stats_and_verifies.md)
- [s3_compatible_backend_put_read_stat_and_verify_round_trip](../../../../../../../functions/crates/lpe-storage/src/blob_store/tests/s3_compatible_backend_put_read_stat_and_verify_round_trip.md)
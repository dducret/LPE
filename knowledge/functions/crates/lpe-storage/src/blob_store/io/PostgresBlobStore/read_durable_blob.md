---
type: Rust Method
title: read_durable_blob
resource: crates/lpe-storage/src/blob_store/io.rs#L16-L60
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/load_active_blob_placement
  - functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/error_if_durable_blob_lacks_active_placement
  - functions/crates/lpe-storage/src/storage_backend/s3_read_object
  called_by:
  - functions/crates/lpe-storage/src/activesync/Storage/fetch_activesync_attachment_content
  - functions/crates/lpe-storage/src/activesync/Storage/fetch_message_attachment_content_by_cid
  - functions/crates/lpe-storage/src/attachments/Storage/fetch_calendar_attachment_blob
  - functions/crates/lpe-storage/src/blob_store/tests/assert_active_blob_read
  - functions/crates/lpe-storage/src/blob_store/tests/copy_verify_worker_leaves_active_source_read_path_unchanged
  - functions/crates/lpe-storage/src/blob_store/tests/switch_preserves_reads_stats_and_verification_across_phases
  - functions/crates/lpe-storage/src/blob_store/tests/durable_blob_store_writes_reads_stats_and_verifies
  - functions/crates/lpe-storage/src/blob_store/tests/s3_compatible_backend_put_read_stat_and_verify_round_trip
  - functions/crates/lpe-storage/src/pst/Storage/export_mailbox_to_pst
  - functions/crates/lpe-storage/src/submission/Storage/fetch_draft_attachment_inputs_in_tx
---

# Signature

`pub(crate) async fn read_durable_blob( &self, pool: &PgPool, tenant_id: &Uuid, domain_id: Uuid, kind: DurableBlobKind, blob_id: Uuid, ) -> Result<Option<StoredBlobBytes>>`

# Calls

- [load_active_blob_placement](../../../../../../../functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/load_active_blob_placement.md)
- [error_if_durable_blob_lacks_active_placement](../../../../../../../functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/error_if_durable_blob_lacks_active_placement.md)
- [s3_read_object](../../../../../../../functions/crates/lpe-storage/src/storage_backend/s3_read_object.md)

# Called by

- [fetch_activesync_attachment_content](../../../../../../../functions/crates/lpe-storage/src/activesync/Storage/fetch_activesync_attachment_content.md)
- [fetch_message_attachment_content_by_cid](../../../../../../../functions/crates/lpe-storage/src/activesync/Storage/fetch_message_attachment_content_by_cid.md)
- [fetch_calendar_attachment_blob](../../../../../../../functions/crates/lpe-storage/src/attachments/Storage/fetch_calendar_attachment_blob.md)
- [assert_active_blob_read](../../../../../../../functions/crates/lpe-storage/src/blob_store/tests/assert_active_blob_read.md)
- [copy_verify_worker_leaves_active_source_read_path_unchanged](../../../../../../../functions/crates/lpe-storage/src/blob_store/tests/copy_verify_worker_leaves_active_source_read_path_unchanged.md)
- [switch_preserves_reads_stats_and_verification_across_phases](../../../../../../../functions/crates/lpe-storage/src/blob_store/tests/switch_preserves_reads_stats_and_verification_across_phases.md)
- [durable_blob_store_writes_reads_stats_and_verifies](../../../../../../../functions/crates/lpe-storage/src/blob_store/tests/durable_blob_store_writes_reads_stats_and_verifies.md)
- [s3_compatible_backend_put_read_stat_and_verify_round_trip](../../../../../../../functions/crates/lpe-storage/src/blob_store/tests/s3_compatible_backend_put_read_stat_and_verify_round_trip.md)
- [export_mailbox_to_pst](../../../../../../../functions/crates/lpe-storage/src/pst/Storage/export_mailbox_to_pst.md)
- [fetch_draft_attachment_inputs_in_tx](../../../../../../../functions/crates/lpe-storage/src/submission/Storage/fetch_draft_attachment_inputs_in_tx.md)
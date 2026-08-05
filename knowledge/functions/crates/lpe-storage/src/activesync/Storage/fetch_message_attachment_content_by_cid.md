---
type: Rust Method
title: fetch_message_attachment_content_by_cid
resource: crates/lpe-storage/src/activesync.rs#L635-L695
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/read_durable_blob
  called_by:
  - functions/crates/lpe-storage/src/blob_store/tests/attachment_content_fetch_reads_through_blob_store_boundary
---

# Signature

`pub async fn fetch_message_attachment_content_by_cid( &self, account_id: Uuid, message_id: Uuid, content_id: &str, ) -> Result<Option<ActiveSyncAttachmentContent>>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [read_durable_blob](../../../../../../functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/read_durable_blob.md)

# Called by

- [attachment_content_fetch_reads_through_blob_store_boundary](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/attachment_content_fetch_reads_through_blob_store_boundary.md)
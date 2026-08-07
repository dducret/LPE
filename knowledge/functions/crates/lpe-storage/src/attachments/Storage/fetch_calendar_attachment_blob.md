---
type: Rust Method
title: fetch_calendar_attachment_blob
resource: crates/lpe-storage/src/attachments.rs#L595-L650
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/attachments/parse_calendar_attachment_file_reference
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/read_durable_blob
---

# Signature

`pub async fn fetch_calendar_attachment_blob( &self, account_id: Uuid, file_reference: &str, ) -> Result<Option<JmapUploadBlob>>`

# Calls

- [parse_calendar_attachment_file_reference](../../../../../../functions/crates/lpe-storage/src/attachments/parse_calendar_attachment_file_reference.md)
- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [read_durable_blob](../../../../../../functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/read_durable_blob.md)
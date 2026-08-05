---
type: Rust Method
title: resolve_upload_source
resource: crates/lpe-jmap/src/blob.rs#L362-L390
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/convert/resolve_creation_reference
  - functions/crates/lpe-jmap/src/service/blobs/JmapService/resolve_download_blob
  - functions/crates/lpe-jmap/src/blob/slice_blob_range
  called_by:
  - functions/crates/lpe-jmap/src/blob/JmapService/build_blob_upload
---

# Signature

`async fn resolve_upload_source( &self, account_access: &MailboxAccountAccess, source: BlobDataSource, created_ids: &HashMap<String, String>, ) -> Result<Vec<u8>>`

# Calls

- [resolve_creation_reference](../../../../../../functions/crates/lpe-jmap/src/convert/resolve_creation_reference.md)
- [resolve_download_blob](../../../../../../functions/crates/lpe-jmap/src/service/blobs/JmapService/resolve_download_blob.md)
- [slice_blob_range](../../../../../../functions/crates/lpe-jmap/src/blob/slice_blob_range.md)

# Called by

- [build_blob_upload](../../../../../../functions/crates/lpe-jmap/src/blob/JmapService/build_blob_upload.md)
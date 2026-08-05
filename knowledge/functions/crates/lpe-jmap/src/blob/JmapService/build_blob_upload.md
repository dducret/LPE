---
type: Rust Method
title: build_blob_upload
resource: crates/lpe-jmap/src/blob.rs#L311-L360
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/blob/JmapService/resolve_upload_source
  called_by:
  - functions/crates/lpe-jmap/src/blob/JmapService/handle_blob_upload
---

# Signature

`async fn build_blob_upload( &self, account_access: &MailboxAccountAccess, upload: BlobUploadObject, created_ids: &HashMap<String, String>, ) -> Result<(String, Vec<u8>)>`

# Calls

- [resolve_upload_source](../../../../../../functions/crates/lpe-jmap/src/blob/JmapService/resolve_upload_source.md)

# Called by

- [handle_blob_upload](../../../../../../functions/crates/lpe-jmap/src/blob/JmapService/handle_blob_upload.md)
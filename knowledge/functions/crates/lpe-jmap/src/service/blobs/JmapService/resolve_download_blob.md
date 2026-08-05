---
type: Rust Method
title: resolve_download_blob
resource: crates/lpe-jmap/src/service/blobs.rs#L64-L71
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/blobs/JmapService/resolve_download_blob_with_bcc
  called_by:
  - functions/crates/lpe-jmap/src/blob/JmapService/handle_blob_get
  - functions/crates/lpe-jmap/src/blob/JmapService/resolve_upload_source
  - functions/crates/lpe-jmap/src/service/blobs/JmapService/handle_download
---

# Signature

`pub(crate) async fn resolve_download_blob( &self, requested_account: &MailboxAccountAccess, blob_id: &str, ) -> Result<JmapUploadBlob>`

# Calls

- [resolve_download_blob_with_bcc](../../../../../../../functions/crates/lpe-jmap/src/service/blobs/JmapService/resolve_download_blob_with_bcc.md)

# Called by

- [handle_blob_get](../../../../../../../functions/crates/lpe-jmap/src/blob/JmapService/handle_blob_get.md)
- [resolve_upload_source](../../../../../../../functions/crates/lpe-jmap/src/blob/JmapService/resolve_upload_source.md)
- [handle_download](../../../../../../../functions/crates/lpe-jmap/src/service/blobs/JmapService/handle_download.md)
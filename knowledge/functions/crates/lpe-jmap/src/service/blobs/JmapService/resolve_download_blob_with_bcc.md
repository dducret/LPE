---
type: Rust Method
title: resolve_download_blob_with_bcc
resource: crates/lpe-jmap/src/service/blobs.rs#L73-L125
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  - functions/crates/lpe-jmap/src/upload/message_rfc822_bytes
  called_by:
  - functions/crates/lpe-jmap/src/blob/JmapService/handle_blob_copy
  - functions/crates/lpe-jmap/src/service/blobs/JmapService/resolve_download_blob
---

# Signature

`pub(crate) async fn resolve_download_blob_with_bcc( &self, requested_account: &MailboxAccountAccess, blob_id: &str, include_bcc: bool, ) -> Result<JmapUploadBlob>`

# Calls

- [next](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)
- [message_rfc822_bytes](../../../../../../../functions/crates/lpe-jmap/src/upload/message_rfc822_bytes.md)

# Called by

- [handle_blob_copy](../../../../../../../functions/crates/lpe-jmap/src/blob/JmapService/handle_blob_copy.md)
- [resolve_download_blob](../../../../../../../functions/crates/lpe-jmap/src/service/blobs/JmapService/resolve_download_blob.md)
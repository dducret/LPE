---
type: Rust Method
title: handle_download
resource: crates/lpe-jmap/src/service/blobs.rs#L50-L62
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/JmapService/requested_account_access
  - functions/crates/lpe-jmap/src/service/blobs/JmapService/resolve_download_blob
  called_by:
  - functions/crates/lpe-jmap/src/service/download_handler
  - functions/crates/lpe-jmap/src/tests/email_get_exposes_canonical_blob_ids_and_download_accepts_upload_prefix
  - functions/crates/lpe-jmap/src/tests/owned_message_download_prefers_sanitized_stored_raw_mime_blob
  - functions/crates/lpe-jmap/src/tests/message_blob_download_hides_bcc_for_delegated_shared_mailbox
  - functions/crates/lpe-jmap/src/tests/upload_and_download_use_authenticated_account
---

# Signature

`pub(crate) async fn handle_download( &self, authorization: Option<&str>, account_id: &str, blob_id: &str, ) -> Result<JmapUploadBlob>`

# Calls

- [requested_account_access](../../../../../../../functions/crates/lpe-jmap/src/service/JmapService/requested_account_access.md)
- [resolve_download_blob](../../../../../../../functions/crates/lpe-jmap/src/service/blobs/JmapService/resolve_download_blob.md)

# Called by

- [download_handler](../../../../../../../functions/crates/lpe-jmap/src/service/download_handler.md)
- [email_get_exposes_canonical_blob_ids_and_download_accepts_upload_prefix](../../../../../../../functions/crates/lpe-jmap/src/tests/email_get_exposes_canonical_blob_ids_and_download_accepts_upload_prefix.md)
- [owned_message_download_prefers_sanitized_stored_raw_mime_blob](../../../../../../../functions/crates/lpe-jmap/src/tests/owned_message_download_prefers_sanitized_stored_raw_mime_blob.md)
- [message_blob_download_hides_bcc_for_delegated_shared_mailbox](../../../../../../../functions/crates/lpe-jmap/src/tests/message_blob_download_hides_bcc_for_delegated_shared_mailbox.md)
- [upload_and_download_use_authenticated_account](../../../../../../../functions/crates/lpe-jmap/src/tests/upload_and_download_use_authenticated_account.md)
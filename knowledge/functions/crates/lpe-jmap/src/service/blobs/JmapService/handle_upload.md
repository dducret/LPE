---
type: Rust Method
title: handle_upload
resource: crates/lpe-jmap/src/service/blobs.rs#L4-L48
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/JmapService/requested_account_access
  called_by:
  - functions/crates/lpe-jmap/src/service/upload_handler
  - functions/crates/lpe-jmap/src/tests/blob_create_paths_reject_read_only_shared_accounts
  - functions/crates/lpe-jmap/src/tests/upload_and_download_use_authenticated_account
  - functions/crates/lpe-jmap/src/tests/upload_rejects_bodies_larger_than_session_limit
  - functions/crates/lpe-jmap/src/tests/upload_accepts_validated_matching_blob
  - functions/crates/lpe-jmap/src/tests/upload_rejects_declared_mime_mismatch
  - functions/crates/lpe-jmap/src/tests/upload_rejects_unknown_type
  - functions/crates/lpe-jmap/src/tests/upload_surfaces_magika_failure_mode
---

# Signature

`pub(crate) async fn handle_upload( &self, authorization: Option<&str>, account_id: &str, media_type: &str, body: &[u8], ) -> Result<Value>`

# Calls

- [requested_account_access](../../../../../../../functions/crates/lpe-jmap/src/service/JmapService/requested_account_access.md)

# Called by

- [upload_handler](../../../../../../../functions/crates/lpe-jmap/src/service/upload_handler.md)
- [blob_create_paths_reject_read_only_shared_accounts](../../../../../../../functions/crates/lpe-jmap/src/tests/blob_create_paths_reject_read_only_shared_accounts.md)
- [upload_and_download_use_authenticated_account](../../../../../../../functions/crates/lpe-jmap/src/tests/upload_and_download_use_authenticated_account.md)
- [upload_rejects_bodies_larger_than_session_limit](../../../../../../../functions/crates/lpe-jmap/src/tests/upload_rejects_bodies_larger_than_session_limit.md)
- [upload_accepts_validated_matching_blob](../../../../../../../functions/crates/lpe-jmap/src/tests/upload_accepts_validated_matching_blob.md)
- [upload_rejects_declared_mime_mismatch](../../../../../../../functions/crates/lpe-jmap/src/tests/upload_rejects_declared_mime_mismatch.md)
- [upload_rejects_unknown_type](../../../../../../../functions/crates/lpe-jmap/src/tests/upload_rejects_unknown_type.md)
- [upload_surfaces_magika_failure_mode](../../../../../../../functions/crates/lpe-jmap/src/tests/upload_surfaces_magika_failure_mode.md)
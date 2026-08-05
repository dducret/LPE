---
type: Rust Method
title: handle_blob_copy
resource: crates/lpe-jmap/src/blob.rs#L253-L309
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/JmapService/requested_account_access
  - functions/crates/lpe-jmap/src/blob/ensure_blob_create_allowed
  - functions/crates/lpe-jmap/src/convert/resolve_creation_reference
  - functions/crates/lpe-jmap/src/service/blobs/JmapService/resolve_download_blob_with_bcc
  - functions/crates/lpe-jmap/src/error/set_error
  called_by:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account
---

# Signature

`pub(crate) async fn handle_blob_copy( &self, account: &AuthenticatedAccount, arguments: Value, created_ids: &HashMap<String, String>, ) -> Result<Value>`

# Calls

- [requested_account_access](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/requested_account_access.md)
- [ensure_blob_create_allowed](../../../../../../functions/crates/lpe-jmap/src/blob/ensure_blob_create_allowed.md)
- [resolve_creation_reference](../../../../../../functions/crates/lpe-jmap/src/convert/resolve_creation_reference.md)
- [resolve_download_blob_with_bcc](../../../../../../functions/crates/lpe-jmap/src/service/blobs/JmapService/resolve_download_blob_with_bcc.md)
- [set_error](../../../../../../functions/crates/lpe-jmap/src/error/set_error.md)

# Called by

- [handle_api_request_for_account](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account.md)
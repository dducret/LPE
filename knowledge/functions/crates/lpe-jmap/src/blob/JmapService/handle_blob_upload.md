---
type: Rust Method
title: handle_blob_upload
resource: crates/lpe-jmap/src/blob.rs#L75-L133
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/JmapService/requested_account_access
  - functions/crates/lpe-jmap/src/blob/ensure_blob_create_allowed
  - functions/crates/lpe-jmap/src/blob/JmapService/build_blob_upload
  - functions/crates/lpe-jmap/src/error/set_error
  called_by:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account
---

# Signature

`pub(crate) async fn handle_blob_upload( &self, account: &AuthenticatedAccount, arguments: Value, created_ids: &mut HashMap<String, String>, ) -> Result<Value>`

# Calls

- [requested_account_access](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/requested_account_access.md)
- [ensure_blob_create_allowed](../../../../../../functions/crates/lpe-jmap/src/blob/ensure_blob_create_allowed.md)
- [build_blob_upload](../../../../../../functions/crates/lpe-jmap/src/blob/JmapService/build_blob_upload.md)
- [set_error](../../../../../../functions/crates/lpe-jmap/src/error/set_error.md)

# Called by

- [handle_api_request_for_account](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account.md)
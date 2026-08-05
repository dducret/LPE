---
type: Rust Method
title: handle_blob_get
resource: crates/lpe-jmap/src/blob.rs#L135-L177
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/JmapService/requested_account_access
  - functions/crates/lpe-jmap/src/blob/unsupported_blob_get_property
  - functions/crates/lpe-jmap/src/convert/resolve_creation_reference
  - functions/crates/lpe-jmap/src/service/blobs/JmapService/resolve_download_blob
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-jmap/src/blob/blob_get_object
  called_by:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account
---

# Signature

`pub(crate) async fn handle_blob_get( &self, account: &AuthenticatedAccount, arguments: Value, created_ids: &HashMap<String, String>, ) -> Result<Value>`

# Calls

- [requested_account_access](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/requested_account_access.md)
- [unsupported_blob_get_property](../../../../../../functions/crates/lpe-jmap/src/blob/unsupported_blob_get_property.md)
- [resolve_creation_reference](../../../../../../functions/crates/lpe-jmap/src/convert/resolve_creation_reference.md)
- [resolve_download_blob](../../../../../../functions/crates/lpe-jmap/src/service/blobs/JmapService/resolve_download_blob.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [blob_get_object](../../../../../../functions/crates/lpe-jmap/src/blob/blob_get_object.md)

# Called by

- [handle_api_request_for_account](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account.md)
---
type: Rust Method
title: handle_blob_lookup
resource: crates/lpe-jmap/src/blob.rs#L179-L251
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/error/method_error
  - functions/crates/lpe-jmap/src/service/JmapService/requested_account_access
  - functions/crates/lpe-jmap/src/blob/blob_lookup_index
  - functions/crates/lpe-jmap/src/convert/resolve_creation_reference
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-jmap/src/blob/sorted_values
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account
---

# Signature

`pub(crate) async fn handle_blob_lookup( &self, account: &AuthenticatedAccount, arguments: Value, created_ids: &HashMap<String, String>, declared_capabilities: &[String], ) -> Result<Value>`

# Calls

- [method_error](../../../../../../functions/crates/lpe-jmap/src/error/method_error.md)
- [requested_account_access](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/requested_account_access.md)
- [blob_lookup_index](../../../../../../functions/crates/lpe-jmap/src/blob/blob_lookup_index.md)
- [resolve_creation_reference](../../../../../../functions/crates/lpe-jmap/src/convert/resolve_creation_reference.md)
- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [sorted_values](../../../../../../functions/crates/lpe-jmap/src/blob/sorted_values.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [handle_api_request_for_account](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account.md)
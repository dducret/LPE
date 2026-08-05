---
type: Rust Method
title: handle_share_import_or_copy
resource: crates/lpe-jmap/src/service.rs#L1258-L1283
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-jmap/src/service/JmapService/handle_share_set
  called_by:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account
---

# Signature

`pub(crate) async fn handle_share_import_or_copy( &self, account: &AuthenticatedAccount, arguments: Value, created_ids: &mut HashMap<String, String>, method_name: &str, ) -> Result<Value>`

# Calls

- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [handle_share_set](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_share_set.md)

# Called by

- [handle_api_request_for_account](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account.md)
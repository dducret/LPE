---
type: Rust Method
title: handle_share_set
resource: crates/lpe-jmap/src/service.rs#L1141-L1256
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/helpers/requested_account_id_from_arguments
  - functions/crates/lpe-jmap/src/service/canonical/JmapService/canonical_object_state
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-jmap/src/service/helpers/parse_share_input
  - functions/crates/lpe-jmap/src/service/helpers/share_audit
  - functions/crates/lpe-jmap/src/error/set_error
  - functions/crates/lpe-jmap/src/service/canonical/JmapService/canonical_objects
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account
  - functions/crates/lpe-jmap/src/service/JmapService/handle_share_import_or_copy
---

# Signature

`pub(crate) async fn handle_share_set( &self, account: &AuthenticatedAccount, arguments: Value, created_ids: &mut HashMap<String, String>, ) -> Result<Value>`

# Calls

- [requested_account_id_from_arguments](../../../../../../functions/crates/lpe-jmap/src/service/helpers/requested_account_id_from_arguments.md)
- [canonical_object_state](../../../../../../functions/crates/lpe-jmap/src/service/canonical/JmapService/canonical_object_state.md)
- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [parse_share_input](../../../../../../functions/crates/lpe-jmap/src/service/helpers/parse_share_input.md)
- [share_audit](../../../../../../functions/crates/lpe-jmap/src/service/helpers/share_audit.md)
- [set_error](../../../../../../functions/crates/lpe-jmap/src/error/set_error.md)
- [canonical_objects](../../../../../../functions/crates/lpe-jmap/src/service/canonical/JmapService/canonical_objects.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [handle_api_request_for_account](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account.md)
- [handle_share_import_or_copy](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_share_import_or_copy.md)
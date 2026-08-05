---
type: Rust Method
title: handle_reminder_set
resource: crates/lpe-jmap/src/service.rs#L895-L1004
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/helpers/requested_account_id_from_arguments
  - functions/crates/lpe-jmap/src/service/canonical/JmapService/canonical_object_state
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-jmap/src/service/JmapService/apply_reminder_mutation
  - functions/crates/lpe-jmap/src/error/set_error
  - functions/crates/lpe-jmap/src/service/helpers/parse_reminder_id
  - functions/crates/lpe-jmap/src/state/entry
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account
  - functions/crates/lpe-jmap/src/service/JmapService/handle_reminder_import_or_copy
---

# Signature

`pub(crate) async fn handle_reminder_set( &self, account: &AuthenticatedAccount, arguments: Value, created_ids: &mut HashMap<String, String>, ) -> Result<Value>`

# Calls

- [requested_account_id_from_arguments](../../../../../../functions/crates/lpe-jmap/src/service/helpers/requested_account_id_from_arguments.md)
- [canonical_object_state](../../../../../../functions/crates/lpe-jmap/src/service/canonical/JmapService/canonical_object_state.md)
- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [apply_reminder_mutation](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/apply_reminder_mutation.md)
- [set_error](../../../../../../functions/crates/lpe-jmap/src/error/set_error.md)
- [parse_reminder_id](../../../../../../functions/crates/lpe-jmap/src/service/helpers/parse_reminder_id.md)
- [entry](../../../../../../functions/crates/lpe-jmap/src/state/entry.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [handle_api_request_for_account](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account.md)
- [handle_reminder_import_or_copy](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_reminder_import_or_copy.md)
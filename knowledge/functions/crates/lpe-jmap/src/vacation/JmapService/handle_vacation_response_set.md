---
type: Rust Method
title: handle_vacation_response_set
resource: crates/lpe-jmap/src/vacation.rs#L75-L184
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/session/requested_account_id
  - functions/crates/lpe-jmap/src/vacation/JmapService/vacation_response_projection
  - functions/crates/lpe-jmap/src/vacation/vacation_response_state
  - functions/crates/lpe-jmap/src/error/set_error
  - functions/crates/lpe-jmap/src/vacation/save_vacation_response
  - functions/crates/lpe-jmap/src/vacation/vacation_response_to_value
  - functions/crates/lpe-jmap/src/vacation/vacation_response_properties
  - functions/crates/lpe-jmap/src/convert/resolve_creation_reference
  - functions/crates/lpe-jmap/src/vacation/vacation_audit
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account
---

# Signature

`pub(crate) async fn handle_vacation_response_set( &self, account: &AuthenticatedAccount, arguments: Value, created_ids: &mut std::collections::HashMap<String, String>, ) -> Result<Value>`

# Calls

- [requested_account_id](../../../../../../functions/crates/lpe-jmap/src/session/requested_account_id.md)
- [vacation_response_projection](../../../../../../functions/crates/lpe-jmap/src/vacation/JmapService/vacation_response_projection.md)
- [vacation_response_state](../../../../../../functions/crates/lpe-jmap/src/vacation/vacation_response_state.md)
- [set_error](../../../../../../functions/crates/lpe-jmap/src/error/set_error.md)
- [save_vacation_response](../../../../../../functions/crates/lpe-jmap/src/vacation/save_vacation_response.md)
- [vacation_response_to_value](../../../../../../functions/crates/lpe-jmap/src/vacation/vacation_response_to_value.md)
- [vacation_response_properties](../../../../../../functions/crates/lpe-jmap/src/vacation/vacation_response_properties.md)
- [resolve_creation_reference](../../../../../../functions/crates/lpe-jmap/src/convert/resolve_creation_reference.md)
- [vacation_audit](../../../../../../functions/crates/lpe-jmap/src/vacation/vacation_audit.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [handle_api_request_for_account](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account.md)
---
type: Rust Method
title: handle_vacation_response_get
resource: crates/lpe-jmap/src/vacation.rs#L43-L73
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/session/requested_account_id
  - functions/crates/lpe-jmap/src/vacation/vacation_response_properties
  - functions/crates/lpe-jmap/src/vacation/JmapService/vacation_response_projection
  - functions/crates/lpe-jmap/src/vacation/vacation_response_state
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-jmap/src/vacation/vacation_response_to_value
  called_by:
  - functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account
---

# Signature

`pub(crate) async fn handle_vacation_response_get( &self, account: &AuthenticatedAccount, arguments: Value, ) -> Result<Value>`

# Calls

- [requested_account_id](../../../../../../functions/crates/lpe-jmap/src/session/requested_account_id.md)
- [vacation_response_properties](../../../../../../functions/crates/lpe-jmap/src/vacation/vacation_response_properties.md)
- [vacation_response_projection](../../../../../../functions/crates/lpe-jmap/src/vacation/JmapService/vacation_response_projection.md)
- [vacation_response_state](../../../../../../functions/crates/lpe-jmap/src/vacation/vacation_response_state.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [vacation_response_to_value](../../../../../../functions/crates/lpe-jmap/src/vacation/vacation_response_to_value.md)

# Called by

- [handle_api_request_for_account](../../../../../../functions/crates/lpe-jmap/src/service/JmapService/handle_api_request_for_account.md)
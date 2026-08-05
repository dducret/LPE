---
type: Rust Method
title: handle_provision
resource: crates/lpe-activesync/src/service/provisioning.rs#L16-L127
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/service/command_status_response
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value
  - functions/crates/lpe-activesync/src/response/policy_key
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text
  - functions/crates/lpe-activesync/src/response/wbxml_response
  called_by:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/handle_parsed_request
---

# Signature

`pub(super) async fn handle_provision( &self, principal: &AuthenticatedPrincipal, device_id: &str, device_type: &str, protocol_version: &str, request: &WbxmlNode, ) -> Result<Response>`

# Calls

- [command_status_response](../../../../../../../functions/crates/lpe-activesync/src/service/command_status_response.md)
- [text_value](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value.md)
- [policy_key](../../../../../../../functions/crates/lpe-activesync/src/response/policy_key.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [with_text](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text.md)
- [wbxml_response](../../../../../../../functions/crates/lpe-activesync/src/response/wbxml_response.md)

# Called by

- [handle_parsed_request](../../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/handle_parsed_request.md)
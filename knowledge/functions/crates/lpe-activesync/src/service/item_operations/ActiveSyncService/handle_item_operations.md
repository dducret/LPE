---
type: Rust Method
title: handle_item_operations
resource: crates/lpe-activesync/src/service/item_operations.rs#L16-L53
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/service/command_status_response
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-activesync/src/service/item_operations/ActiveSyncService/handle_item_operations_fetch
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text
  - functions/crates/lpe-activesync/src/response/wbxml_response
  called_by:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/handle_parsed_request
---

# Signature

`pub(super) async fn handle_item_operations( &self, principal: &AuthenticatedPrincipal, protocol_version: &str, request: &WbxmlNode, ) -> Result<Response>`

# Calls

- [command_status_response](../../../../../../../functions/crates/lpe-activesync/src/service/command_status_response.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [handle_item_operations_fetch](../../../../../../../functions/crates/lpe-activesync/src/service/item_operations/ActiveSyncService/handle_item_operations_fetch.md)
- [with_text](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text.md)
- [wbxml_response](../../../../../../../functions/crates/lpe-activesync/src/response/wbxml_response.md)

# Called by

- [handle_parsed_request](../../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/handle_parsed_request.md)
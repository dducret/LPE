---
type: Rust Method
title: handle_move_items
resource: crates/lpe-activesync/src/service/move_items.rs#L17-L40
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/service/command_status_response
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-activesync/src/service/move_items/ActiveSyncService/handle_move_item
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text
  - functions/crates/lpe-activesync/src/response/wbxml_response
  called_by:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/handle_parsed_request
---

# Signature

`pub(super) async fn handle_move_items( &self, principal: &AuthenticatedPrincipal, protocol_version: &str, request: &WbxmlNode, ) -> Result<Response>`

# Calls

- [command_status_response](../../../../../../../functions/crates/lpe-activesync/src/service/command_status_response.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [handle_move_item](../../../../../../../functions/crates/lpe-activesync/src/service/move_items/ActiveSyncService/handle_move_item.md)
- [with_text](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text.md)
- [wbxml_response](../../../../../../../functions/crates/lpe-activesync/src/response/wbxml_response.md)

# Called by

- [handle_parsed_request](../../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/handle_parsed_request.md)
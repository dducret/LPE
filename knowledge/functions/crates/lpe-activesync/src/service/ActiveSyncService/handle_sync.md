---
type: Rust Method
title: handle_sync
resource: crates/lpe-activesync/src/service.rs#L332-L359
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/snapshot/require_sync_collections
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text
  - functions/crates/lpe-activesync/src/response/wbxml_response
  called_by:
  - functions/crates/lpe-activesync/src/service/ActiveSyncService/handle_parsed_request
---

# Signature

`async fn handle_sync( &self, principal: &AuthenticatedPrincipal, device_id: &str, protocol_version: &str, request: &WbxmlNode, ) -> Result<Response>`

# Calls

- [require_sync_collections](../../../../../../functions/crates/lpe-activesync/src/snapshot/require_sync_collections.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [with_text](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text.md)
- [wbxml_response](../../../../../../functions/crates/lpe-activesync/src/response/wbxml_response.md)

# Called by

- [handle_parsed_request](../../../../../../functions/crates/lpe-activesync/src/service/ActiveSyncService/handle_parsed_request.md)
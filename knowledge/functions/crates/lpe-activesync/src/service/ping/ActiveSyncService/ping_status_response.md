---
type: Rust Method
title: ping_status_response
resource: crates/lpe-activesync/src/service/ping.rs#L205-L238
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text
  - functions/crates/lpe-activesync/src/response/wbxml_response
  called_by:
  - functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/handle_ping
---

# Signature

`fn ping_status_response( &self, protocol_version: &str, status: &str, heartbeat_interval: Option<u32>, max_folders: Option<usize>, changed: &[String], ) -> Result<Response>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [with_text](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/with_text.md)
- [wbxml_response](../../../../../../../functions/crates/lpe-activesync/src/response/wbxml_response.md)

# Called by

- [handle_ping](../../../../../../../functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/handle_ping.md)
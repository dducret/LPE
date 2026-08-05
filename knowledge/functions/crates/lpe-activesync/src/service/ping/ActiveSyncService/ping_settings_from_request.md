---
type: Rust Method
title: ping_settings_from_request
resource: crates/lpe-activesync/src/service/ping.rs#L240-L276
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/handle_ping
---

# Signature

`fn ping_settings_from_request( &self, request: &WbxmlNode, cached: Option<&PingSettings>, ) -> Option<PingSettings>`

# Calls

- [text_value](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/text_value.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [handle_ping](../../../../../../../functions/crates/lpe-activesync/src/service/ping/ActiveSyncService/handle_ping.md)
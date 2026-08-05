---
type: Rust Function
title: websocket_request_id
resource: crates/lpe-jmap/src/websocket.rs#L720-L725
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-jmap/src/websocket/JmapService/handle_websocket_message
---

# Signature

`fn websocket_request_id(value: &Value) -> Option<String>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [handle_websocket_message](../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/handle_websocket_message.md)
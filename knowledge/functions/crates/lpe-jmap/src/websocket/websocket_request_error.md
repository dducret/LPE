---
type: Rust Function
title: websocket_request_error
resource: crates/lpe-jmap/src/websocket.rs#L727-L740
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/websocket/JmapService/send_request_error
  - functions/crates/lpe-jmap/src/websocket/parse_websocket_object
---

# Signature

`fn websocket_request_error( request_id: Option<String>, error_type: &str, status: StatusCode, detail: &str, ) -> WebSocketRequestError`

# Called by

- [send_request_error](../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/send_request_error.md)
- [parse_websocket_object](../../../../../functions/crates/lpe-jmap/src/websocket/parse_websocket_object.md)
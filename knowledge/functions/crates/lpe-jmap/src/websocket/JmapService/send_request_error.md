---
type: Rust Method
title: send_request_error
resource: crates/lpe-jmap/src/websocket.rs#L546-L559
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/websocket/JmapService/send_request_error_object
  - functions/crates/lpe-jmap/src/websocket/websocket_request_error
  called_by:
  - functions/crates/lpe-jmap/src/websocket/JmapService/handle_websocket_message
---

# Signature

`async fn send_request_error( &self, socket: &mut WebSocket, request_id: Option<String>, error_type: &str, status: StatusCode, detail: &str, ) -> Result<()>`

# Calls

- [send_request_error_object](../../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/send_request_error_object.md)
- [websocket_request_error](../../../../../../functions/crates/lpe-jmap/src/websocket/websocket_request_error.md)

# Called by

- [handle_websocket_message](../../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/handle_websocket_message.md)
---
type: Rust Method
title: send_request_error_object
resource: crates/lpe-jmap/src/websocket.rs#L535-L544
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/websocket/JmapService/handle_websocket_message
  - functions/crates/lpe-jmap/src/websocket/JmapService/send_request_error
---

# Signature

`async fn send_request_error_object( &self, socket: &mut WebSocket, error: WebSocketRequestError, ) -> Result<()>`

# Called by

- [handle_websocket_message](../../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/handle_websocket_message.md)
- [send_request_error](../../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/send_request_error.md)
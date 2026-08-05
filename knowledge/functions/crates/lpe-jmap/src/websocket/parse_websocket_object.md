---
type: Rust Function
title: parse_websocket_object
resource: crates/lpe-jmap/src/websocket.rs#L705-L718
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/websocket/websocket_request_error
  called_by:
  - functions/crates/lpe-jmap/src/websocket/JmapService/handle_websocket_message
---

# Signature

`fn parse_websocket_object<T: DeserializeOwned>( value: Value, request_id: Option<String>, detail: &str, ) -> Result<T, WebSocketRequestError>`

# Calls

- [websocket_request_error](../../../../../functions/crates/lpe-jmap/src/websocket/websocket_request_error.md)

# Called by

- [handle_websocket_message](../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/handle_websocket_message.md)
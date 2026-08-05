---
type: Rust Function
title: websocket_handler
resource: crates/lpe-jmap/src/service.rs#L266-L280
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/service/helpers/authorization_header
  - functions/crates/lpe-jmap/src/websocket/JmapService/handle_websocket
---

# Signature

`async fn websocket_handler( ws: WebSocketUpgrade, State(storage): State<Storage>, headers: HeaderMap, ) -> std::result::Result<impl IntoResponse, (StatusCode, Json<Value>)>`

# Calls

- [authorization_header](../../../../../functions/crates/lpe-jmap/src/service/helpers/authorization_header.md)
- [handle_websocket](../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/handle_websocket.md)
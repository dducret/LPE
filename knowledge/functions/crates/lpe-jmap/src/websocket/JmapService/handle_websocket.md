---
type: Rust Method
title: handle_websocket
resource: crates/lpe-jmap/src/websocket.rs#L50-L94
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/websocket/JmapService/push_categories
  called_by:
  - functions/crates/lpe-jmap/src/service/websocket_handler
---

# Signature

`pub(crate) async fn handle_websocket( &self, mut socket: WebSocket, account: AuthenticatedAccount, )`

# Calls

- [push_categories](../../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/push_categories.md)

# Called by

- [websocket_handler](../../../../../../functions/crates/lpe-jmap/src/service/websocket_handler.md)
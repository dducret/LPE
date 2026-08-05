---
type: Rust Method
title: enable_push
resource: crates/lpe-jmap/src/websocket.rs#L257-L286
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/websocket/JmapService/current_push_states
  - functions/crates/lpe-jmap/src/state/encode_push_state
  - functions/crates/lpe-jmap/src/websocket/JmapService/recover_push_enable_change
  - functions/crates/lpe-jmap/src/websocket/JmapService/send_state_change
  called_by:
  - functions/crates/lpe-jmap/src/websocket/JmapService/handle_websocket_message
---

# Signature

`async fn enable_push( &self, socket: &mut WebSocket, account_id: Uuid, subscription: &mut PushSubscription, client_push_state: Option<String>, ) -> Result<()>`

# Calls

- [current_push_states](../../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/current_push_states.md)
- [encode_push_state](../../../../../../functions/crates/lpe-jmap/src/state/encode_push_state.md)
- [recover_push_enable_change](../../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/recover_push_enable_change.md)
- [send_state_change](../../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/send_state_change.md)

# Called by

- [handle_websocket_message](../../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/handle_websocket_message.md)
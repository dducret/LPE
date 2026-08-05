---
type: Rust Method
title: send_state_change
resource: crates/lpe-jmap/src/websocket.rs#L561-L576
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/websocket/JmapService/enable_push
  - functions/crates/lpe-jmap/src/websocket/JmapService/publish_state_changes
---

# Signature

`async fn send_state_change( &self, socket: &mut WebSocket, changed: HashMap<String, HashMap<String, String>>, push_state: String, ) -> Result<()>`

# Called by

- [enable_push](../../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/enable_push.md)
- [publish_state_changes](../../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/publish_state_changes.md)
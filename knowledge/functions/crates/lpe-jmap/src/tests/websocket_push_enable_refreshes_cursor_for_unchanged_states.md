---
type: Rust Function
title: websocket_push_enable_refreshes_cursor_for_unchanged_states
resource: crates/lpe-jmap/src/tests.rs#L11057-L11085
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/websocket/JmapService/current_push_states
  - functions/crates/lpe-jmap/src/state/encode_push_state
  - functions/crates/lpe-jmap/src/websocket/JmapService/recover_push_enable_change
---

# Signature

`async fn websocket_push_enable_refreshes_cursor_for_unchanged_states()`

# Calls

- [current_push_states](../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/current_push_states.md)
- [encode_push_state](../../../../../functions/crates/lpe-jmap/src/state/encode_push_state.md)
- [recover_push_enable_change](../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/recover_push_enable_change.md)
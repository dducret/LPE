---
type: Rust Function
title: websocket_reconnect_falls_back_to_full_snapshot_when_journal_replay_is_truncated
resource: crates/lpe-jmap/src/tests.rs#L11204-L11250
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

`async fn websocket_reconnect_falls_back_to_full_snapshot_when_journal_replay_is_truncated()`

# Calls

- [current_push_states](../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/current_push_states.md)
- [encode_push_state](../../../../../functions/crates/lpe-jmap/src/state/encode_push_state.md)
- [recover_push_enable_change](../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/recover_push_enable_change.md)
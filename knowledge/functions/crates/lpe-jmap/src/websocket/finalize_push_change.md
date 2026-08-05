---
type: Rust Function
title: finalize_push_change
resource: crates/lpe-jmap/src/websocket.rs#L771-L788
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/websocket/merge_journal_cursor
  - functions/crates/lpe-jmap/src/state/encode_push_state
  called_by:
  - functions/crates/lpe-jmap/src/eventsource/JmapService/handle_event_source
  - functions/crates/lpe-jmap/src/websocket/JmapService/publish_state_changes
  - functions/crates/lpe-jmap/src/websocket/finalize_push_change_emits_cursor_only_push_state
---

# Signature

`pub(crate) fn finalize_push_change( subscription: &mut PushSubscription, changed: HashMap<String, HashMap<String, String>>, current_type_states: HashMap<String, HashMap<String, String>>, change_cursor: Option<i64>, ) -> Result<Option<(HashMap<String, HashMap<String, String>>, String)>>`

# Calls

- [merge_journal_cursor](../../../../../functions/crates/lpe-jmap/src/websocket/merge_journal_cursor.md)
- [encode_push_state](../../../../../functions/crates/lpe-jmap/src/state/encode_push_state.md)

# Called by

- [handle_event_source](../../../../../functions/crates/lpe-jmap/src/eventsource/JmapService/handle_event_source.md)
- [publish_state_changes](../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/publish_state_changes.md)
- [finalize_push_change_emits_cursor_only_push_state](../../../../../functions/crates/lpe-jmap/src/websocket/finalize_push_change_emits_cursor_only_push_state.md)
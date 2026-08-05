---
type: Rust Function
title: encode_push_state
resource: crates/lpe-jmap/src/state.rs#L366-L386
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/eventsource/JmapService/handle_event_source
  - functions/crates/lpe-jmap/src/tests/push_subscription
  - functions/crates/lpe-jmap/src/tests/websocket_push_enable_sends_full_state_for_missing_or_stale_push_state
  - functions/crates/lpe-jmap/src/tests/websocket_push_enable_refreshes_cursor_for_unchanged_states
  - functions/crates/lpe-jmap/src/tests/websocket_reconnect_recovers_task_changes_from_canonical_journal
  - functions/crates/lpe-jmap/src/tests/websocket_reconnect_recovers_delegated_mailbox_right_changes_from_journal
  - functions/crates/lpe-jmap/src/tests/websocket_reconnect_falls_back_to_full_snapshot_when_journal_replay_is_truncated
  - functions/crates/lpe-jmap/src/websocket/JmapService/enable_push
  - functions/crates/lpe-jmap/src/websocket/finalize_push_change
  - functions/crates/lpe-jmap/src/websocket/finalize_push_change_emits_cursor_only_push_state
---

# Signature

`pub(crate) fn encode_push_state( type_states: &HashMap<String, HashMap<String, String>>, cursor: Option<i64>, ) -> Result<String>`

# Called by

- [handle_event_source](../../../../../functions/crates/lpe-jmap/src/eventsource/JmapService/handle_event_source.md)
- [push_subscription](../../../../../functions/crates/lpe-jmap/src/tests/push_subscription.md)
- [websocket_push_enable_sends_full_state_for_missing_or_stale_push_state](../../../../../functions/crates/lpe-jmap/src/tests/websocket_push_enable_sends_full_state_for_missing_or_stale_push_state.md)
- [websocket_push_enable_refreshes_cursor_for_unchanged_states](../../../../../functions/crates/lpe-jmap/src/tests/websocket_push_enable_refreshes_cursor_for_unchanged_states.md)
- [websocket_reconnect_recovers_task_changes_from_canonical_journal](../../../../../functions/crates/lpe-jmap/src/tests/websocket_reconnect_recovers_task_changes_from_canonical_journal.md)
- [websocket_reconnect_recovers_delegated_mailbox_right_changes_from_journal](../../../../../functions/crates/lpe-jmap/src/tests/websocket_reconnect_recovers_delegated_mailbox_right_changes_from_journal.md)
- [websocket_reconnect_falls_back_to_full_snapshot_when_journal_replay_is_truncated](../../../../../functions/crates/lpe-jmap/src/tests/websocket_reconnect_falls_back_to_full_snapshot_when_journal_replay_is_truncated.md)
- [enable_push](../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/enable_push.md)
- [finalize_push_change](../../../../../functions/crates/lpe-jmap/src/websocket/finalize_push_change.md)
- [finalize_push_change_emits_cursor_only_push_state](../../../../../functions/crates/lpe-jmap/src/websocket/finalize_push_change_emits_cursor_only_push_state.md)
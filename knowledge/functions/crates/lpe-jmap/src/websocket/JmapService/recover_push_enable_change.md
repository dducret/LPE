---
type: Rust Method
title: recover_push_enable_change
resource: crates/lpe-jmap/src/websocket.rs#L309-L380
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/state/decode_push_state
  - functions/crates/lpe-jmap/src/websocket/filter_push_state_types
  - functions/crates/lpe-jmap/src/state/push_state_entries_to_types
  - functions/crates/lpe-jmap/src/websocket/JmapService/push_categories
  - functions/crates/lpe-jmap/src/websocket/JmapService/compute_push_changes
  called_by:
  - functions/crates/lpe-jmap/src/eventsource/JmapService/handle_event_source
  - functions/crates/lpe-jmap/src/tests/websocket_push_enable_sends_full_state_for_missing_or_stale_push_state
  - functions/crates/lpe-jmap/src/tests/websocket_push_enable_refreshes_cursor_for_unchanged_states
  - functions/crates/lpe-jmap/src/tests/websocket_reconnect_recovers_task_changes_from_canonical_journal
  - functions/crates/lpe-jmap/src/tests/websocket_reconnect_recovers_delegated_mailbox_right_changes_from_journal
  - functions/crates/lpe-jmap/src/tests/websocket_reconnect_falls_back_to_full_snapshot_when_journal_replay_is_truncated
  - functions/crates/lpe-jmap/src/websocket/JmapService/enable_push
---

# Signature

`pub(crate) async fn recover_push_enable_change( &self, principal_account_id: Uuid, enabled_types: &HashSet<String>, client_push_state: Option<&str>, current_cursor: Option<i64>, current_type_states: &HashMap<String, HashMap<String, String>>, ) -> Result<Option<HashMap<String, HashMap<String, String>>>>`

# Calls

- [decode_push_state](../../../../../../functions/crates/lpe-jmap/src/state/decode_push_state.md)
- [filter_push_state_types](../../../../../../functions/crates/lpe-jmap/src/websocket/filter_push_state_types.md)
- [push_state_entries_to_types](../../../../../../functions/crates/lpe-jmap/src/state/push_state_entries_to_types.md)
- [push_categories](../../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/push_categories.md)
- [compute_push_changes](../../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/compute_push_changes.md)

# Called by

- [handle_event_source](../../../../../../functions/crates/lpe-jmap/src/eventsource/JmapService/handle_event_source.md)
- [websocket_push_enable_sends_full_state_for_missing_or_stale_push_state](../../../../../../functions/crates/lpe-jmap/src/tests/websocket_push_enable_sends_full_state_for_missing_or_stale_push_state.md)
- [websocket_push_enable_refreshes_cursor_for_unchanged_states](../../../../../../functions/crates/lpe-jmap/src/tests/websocket_push_enable_refreshes_cursor_for_unchanged_states.md)
- [websocket_reconnect_recovers_task_changes_from_canonical_journal](../../../../../../functions/crates/lpe-jmap/src/tests/websocket_reconnect_recovers_task_changes_from_canonical_journal.md)
- [websocket_reconnect_recovers_delegated_mailbox_right_changes_from_journal](../../../../../../functions/crates/lpe-jmap/src/tests/websocket_reconnect_recovers_delegated_mailbox_right_changes_from_journal.md)
- [websocket_reconnect_falls_back_to_full_snapshot_when_journal_replay_is_truncated](../../../../../../functions/crates/lpe-jmap/src/tests/websocket_reconnect_falls_back_to_full_snapshot_when_journal_replay_is_truncated.md)
- [enable_push](../../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/enable_push.md)
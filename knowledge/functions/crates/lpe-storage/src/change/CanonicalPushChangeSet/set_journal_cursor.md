---
type: Rust Method
title: set_journal_cursor
resource: crates/lpe-storage/src/change.rs#L106-L108
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/tests/websocket_reconnect_recovers_task_changes_from_canonical_journal
  - functions/crates/lpe-jmap/src/tests/websocket_reconnect_recovers_delegated_mailbox_right_changes_from_journal
  - functions/crates/lpe-storage/src/change/CanonicalChangeListener/wait_for_change
  - functions/crates/lpe-storage/src/change/Storage/replay_canonical_changes
---

# Signature

`pub fn set_journal_cursor(&mut self, journal_cursor: i64)`

# Called by

- [websocket_reconnect_recovers_task_changes_from_canonical_journal](../../../../../../functions/crates/lpe-jmap/src/tests/websocket_reconnect_recovers_task_changes_from_canonical_journal.md)
- [websocket_reconnect_recovers_delegated_mailbox_right_changes_from_journal](../../../../../../functions/crates/lpe-jmap/src/tests/websocket_reconnect_recovers_delegated_mailbox_right_changes_from_journal.md)
- [wait_for_change](../../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeListener/wait_for_change.md)
- [replay_canonical_changes](../../../../../../functions/crates/lpe-storage/src/change/Storage/replay_canonical_changes.md)
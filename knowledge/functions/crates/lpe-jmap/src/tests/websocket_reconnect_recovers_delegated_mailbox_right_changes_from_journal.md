---
type: Rust Function
title: websocket_reconnect_recovers_delegated_mailbox_right_changes_from_journal
resource: crates/lpe-jmap/src/tests.rs#L11632-L11692
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/tests/FakeStore/shared_account
  - functions/crates/lpe-jmap/src/websocket/JmapService/current_push_states
  - functions/crates/lpe-jmap/src/state/encode_push_state
  - functions/crates/lpe-storage/src/change/CanonicalPushChangeSet/insert_accounts
  - functions/crates/lpe-storage/src/change/CanonicalPushChangeSet/set_journal_cursor
  - functions/crates/lpe-jmap/src/websocket/JmapService/recover_push_enable_change
---

# Signature

`async fn websocket_reconnect_recovers_delegated_mailbox_right_changes_from_journal()`

# Calls

- [shared_account](../../../../../functions/crates/lpe-jmap/src/tests/FakeStore/shared_account.md)
- [current_push_states](../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/current_push_states.md)
- [encode_push_state](../../../../../functions/crates/lpe-jmap/src/state/encode_push_state.md)
- [insert_accounts](../../../../../functions/crates/lpe-storage/src/change/CanonicalPushChangeSet/insert_accounts.md)
- [set_journal_cursor](../../../../../functions/crates/lpe-storage/src/change/CanonicalPushChangeSet/set_journal_cursor.md)
- [recover_push_enable_change](../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/recover_push_enable_change.md)
---
type: Rust Method
title: publish_state_changes
resource: crates/lpe-jmap/src/websocket.rs#L288-L307
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/websocket/JmapService/compute_push_changes
  - functions/crates/lpe-jmap/src/websocket/finalize_push_change
  - functions/crates/lpe-storage/src/change/CanonicalPushChangeSet/journal_cursor
  - functions/crates/lpe-jmap/src/websocket/JmapService/send_state_change
---

# Signature

`pub(crate) async fn publish_state_changes( &self, socket: &mut WebSocket, principal_account_id: Uuid, subscription: &mut PushSubscription, change_set: &CanonicalPushChangeSet, ) -> Result<()>`

# Calls

- [compute_push_changes](../../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/compute_push_changes.md)
- [finalize_push_change](../../../../../../functions/crates/lpe-jmap/src/websocket/finalize_push_change.md)
- [journal_cursor](../../../../../../functions/crates/lpe-storage/src/change/CanonicalPushChangeSet/journal_cursor.md)
- [send_state_change](../../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/send_state_change.md)
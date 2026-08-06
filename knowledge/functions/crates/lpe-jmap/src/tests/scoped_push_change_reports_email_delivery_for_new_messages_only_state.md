---
type: Rust Function
title: scoped_push_change_reports_email_delivery_for_new_messages_only_state
resource: crates/lpe-jmap/src/tests.rs#L11906-L11964
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/websocket/JmapService/current_push_states
  - functions/crates/lpe-storage/src/change/CanonicalPushChangeSet/insert_accounts
  - functions/crates/lpe-jmap/src/websocket/JmapService/compute_push_changes
  - functions/crates/lpe-jmap/src/tests/push_subscription
  - functions/crates/lpe-jmap/src/tests/FakeStore/draft_email
---

# Signature

`async fn scoped_push_change_reports_email_delivery_for_new_messages_only_state()`

# Calls

- [current_push_states](../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/current_push_states.md)
- [insert_accounts](../../../../../functions/crates/lpe-storage/src/change/CanonicalPushChangeSet/insert_accounts.md)
- [compute_push_changes](../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/compute_push_changes.md)
- [push_subscription](../../../../../functions/crates/lpe-jmap/src/tests/push_subscription.md)
- [draft_email](../../../../../functions/crates/lpe-jmap/src/tests/FakeStore/draft_email.md)
---
type: Rust Function
title: shared_task_push_change_wakes_grantee_principal
resource: crates/lpe-jmap/src/tests.rs#L12056-L12139
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/tests/FakeStore/shared_account
  - functions/crates/lpe-jmap/src/websocket/JmapService/current_push_states
  - functions/crates/lpe-jmap/src/tests/push_subscription
  - functions/crates/lpe-storage/src/change/CanonicalPushChangeSet/insert_accounts
  - functions/crates/lpe-jmap/src/websocket/JmapService/compute_push_changes
---

# Signature

`async fn shared_task_push_change_wakes_grantee_principal()`

# Calls

- [shared_account](../../../../../functions/crates/lpe-jmap/src/tests/FakeStore/shared_account.md)
- [current_push_states](../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/current_push_states.md)
- [push_subscription](../../../../../functions/crates/lpe-jmap/src/tests/push_subscription.md)
- [insert_accounts](../../../../../functions/crates/lpe-storage/src/change/CanonicalPushChangeSet/insert_accounts.md)
- [compute_push_changes](../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/compute_push_changes.md)
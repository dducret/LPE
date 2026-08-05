---
type: Rust Function
title: benchmark_mailbox_listing_and_push_paths
resource: crates/lpe-jmap/src/tests.rs#L14861-L15023
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/tests/legacy_not_found
  - functions/crates/lpe-jmap/src/tests/optimized_not_found
  - functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_query
  - functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_get
  - functions/crates/lpe-jmap/src/websocket/JmapService/current_push_states
  - functions/crates/lpe-jmap/src/tests/push_subscription
  - functions/crates/lpe-storage/src/change/CanonicalPushChangeSet/insert_accounts
  - functions/crates/lpe-jmap/src/websocket/JmapService/compute_push_changes
---

# Signature

`async fn benchmark_mailbox_listing_and_push_paths()`

# Calls

- [legacy_not_found](../../../../../functions/crates/lpe-jmap/src/tests/legacy_not_found.md)
- [optimized_not_found](../../../../../functions/crates/lpe-jmap/src/tests/optimized_not_found.md)
- [handle_mailbox_query](../../../../../functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_query.md)
- [handle_mailbox_get](../../../../../functions/crates/lpe-jmap/src/mailboxes/JmapService/handle_mailbox_get.md)
- [current_push_states](../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/current_push_states.md)
- [push_subscription](../../../../../functions/crates/lpe-jmap/src/tests/push_subscription.md)
- [insert_accounts](../../../../../functions/crates/lpe-storage/src/change/CanonicalPushChangeSet/insert_accounts.md)
- [compute_push_changes](../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/compute_push_changes.md)
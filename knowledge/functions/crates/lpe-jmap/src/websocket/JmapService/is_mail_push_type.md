---
type: Rust Method
title: is_mail_push_type
resource: crates/lpe-jmap/src/websocket.rs#L613-L618
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/websocket/JmapService/compute_push_changes
  - functions/crates/lpe-jmap/src/websocket/JmapService/push_categories
  - functions/crates/lpe-jmap/src/websocket/JmapService/current_push_states
---

# Signature

`fn is_mail_push_type(&self, data_type: &str) -> bool`

# Called by

- [compute_push_changes](../../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/compute_push_changes.md)
- [push_categories](../../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/push_categories.md)
- [current_push_states](../../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/current_push_states.md)
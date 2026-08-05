---
type: Rust Method
title: journal_cursor
resource: crates/lpe-storage/src/change.rs#L110-L112
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/eventsource/JmapService/handle_event_source
  - functions/crates/lpe-jmap/src/websocket/JmapService/publish_state_changes
---

# Signature

`pub fn journal_cursor(&self) -> Option<i64>`

# Called by

- [handle_event_source](../../../../../../functions/crates/lpe-jmap/src/eventsource/JmapService/handle_event_source.md)
- [publish_state_changes](../../../../../../functions/crates/lpe-jmap/src/websocket/JmapService/publish_state_changes.md)
---
type: Rust Function
title: merge_journal_cursor
resource: crates/lpe-jmap/src/websocket.rs#L762-L769
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/websocket/finalize_push_change
---

# Signature

`fn merge_journal_cursor(left: Option<i64>, right: Option<i64>) -> Option<i64>`

# Called by

- [finalize_push_change](../../../../../functions/crates/lpe-jmap/src/websocket/finalize_push_change.md)
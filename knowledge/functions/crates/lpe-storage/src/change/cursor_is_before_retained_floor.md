---
type: Rust Function
title: cursor_is_before_retained_floor
resource: crates/lpe-storage/src/change.rs#L731-L740
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/change/Storage/replay_canonical_changes
---

# Signature

`fn cursor_is_before_retained_floor( after_cursor: i64, earliest_retained_cursor: Option<i64>, current_cursor: Option<i64>, ) -> bool`

# Called by

- [replay_canonical_changes](../../../../../functions/crates/lpe-storage/src/change/Storage/replay_canonical_changes.md)
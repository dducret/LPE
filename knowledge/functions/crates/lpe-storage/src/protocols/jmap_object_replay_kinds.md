---
type: Rust Function
title: jmap_object_replay_kinds
resource: crates/lpe-storage/src/protocols.rs#L1368-L1391
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/protocols/Storage/fetch_jmap_object_change_cursor
  - functions/crates/lpe-storage/src/protocols/Storage/replay_jmap_object_changes
  - functions/crates/lpe-storage/src/protocols/Storage/replay_jmap_string_object_changes
---

# Signature

`fn jmap_object_replay_kinds(data_type: &str) -> Option<Vec<&'static str>>`

# Called by

- [fetch_jmap_object_change_cursor](../../../../../functions/crates/lpe-storage/src/protocols/Storage/fetch_jmap_object_change_cursor.md)
- [replay_jmap_object_changes](../../../../../functions/crates/lpe-storage/src/protocols/Storage/replay_jmap_object_changes.md)
- [replay_jmap_string_object_changes](../../../../../functions/crates/lpe-storage/src/protocols/Storage/replay_jmap_string_object_changes.md)
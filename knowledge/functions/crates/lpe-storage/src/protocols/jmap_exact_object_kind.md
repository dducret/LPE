---
type: Rust Function
title: jmap_exact_object_kind
resource: crates/lpe-storage/src/protocols.rs#L1331-L1346
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/protocols/Storage/replay_jmap_object_changes
  - functions/crates/lpe-storage/src/protocols/jmap_replay_object_id
---

# Signature

`fn jmap_exact_object_kind(data_type: &str) -> Option<&'static str>`

# Called by

- [replay_jmap_object_changes](../../../../../functions/crates/lpe-storage/src/protocols/Storage/replay_jmap_object_changes.md)
- [jmap_replay_object_id](../../../../../functions/crates/lpe-storage/src/protocols/jmap_replay_object_id.md)
---
type: Rust Function
title: jmap_change_kind
resource: crates/lpe-storage/src/protocols.rs#L1339-L1348
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/protocols/Storage/replay_jmap_mail_object_changes
  - functions/crates/lpe-storage/src/protocols/Storage/replay_jmap_object_changes
  - functions/crates/lpe-storage/src/protocols/Storage/replay_jmap_string_object_changes
---

# Signature

`fn jmap_change_kind(change_kind: &str) -> String`

# Called by

- [replay_jmap_mail_object_changes](../../../../../functions/crates/lpe-storage/src/protocols/Storage/replay_jmap_mail_object_changes.md)
- [replay_jmap_object_changes](../../../../../functions/crates/lpe-storage/src/protocols/Storage/replay_jmap_object_changes.md)
- [replay_jmap_string_object_changes](../../../../../functions/crates/lpe-storage/src/protocols/Storage/replay_jmap_string_object_changes.md)
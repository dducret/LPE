---
type: Rust Function
title: jmap_replay_object_id
resource: crates/lpe-storage/src/protocols.rs#L1347-L1365
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/protocols/jmap_exact_object_kind
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-storage/src/protocols/Storage/replay_jmap_object_changes
---

# Signature

`fn jmap_replay_object_id( data_type: &str, object_kind: &str, object_id: Uuid, summary_json: &Value, ) -> Option<Uuid>`

# Calls

- [jmap_exact_object_kind](../../../../../functions/crates/lpe-storage/src/protocols/jmap_exact_object_kind.md)
- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [replay_jmap_object_changes](../../../../../functions/crates/lpe-storage/src/protocols/Storage/replay_jmap_object_changes.md)
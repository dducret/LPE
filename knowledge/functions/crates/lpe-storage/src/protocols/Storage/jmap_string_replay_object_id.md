---
type: Rust Method
title: jmap_string_replay_object_id
resource: crates/lpe-storage/src/protocols.rs#L611-L655
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-storage/src/protocols/Storage/task_share_type_for_collection
  - functions/crates/lpe-storage/src/protocols/summary_json_reminder_changed
  called_by:
  - functions/crates/lpe-storage/src/protocols/Storage/replay_jmap_string_object_changes
---

# Signature

`async fn jmap_string_replay_object_id( &self, tenant_id: &Uuid, data_type: &str, object_kind: &str, object_id: Uuid, summary_json: &Value, ) -> Result<Option<String>>`

# Calls

- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [task_share_type_for_collection](../../../../../../functions/crates/lpe-storage/src/protocols/Storage/task_share_type_for_collection.md)
- [summary_json_reminder_changed](../../../../../../functions/crates/lpe-storage/src/protocols/summary_json_reminder_changed.md)

# Called by

- [replay_jmap_string_object_changes](../../../../../../functions/crates/lpe-storage/src/protocols/Storage/replay_jmap_string_object_changes.md)
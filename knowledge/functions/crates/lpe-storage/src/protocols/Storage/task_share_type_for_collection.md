---
type: Rust Method
title: task_share_type_for_collection
resource: crates/lpe-storage/src/protocols.rs#L656-L679
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/protocols/Storage/jmap_string_replay_object_id
---

# Signature

`async fn task_share_type_for_collection( &self, tenant_id: &Uuid, task_list_id: Uuid, ) -> Result<&'static str>`

# Called by

- [jmap_string_replay_object_id](../../../../../../functions/crates/lpe-storage/src/protocols/Storage/jmap_string_replay_object_id.md)
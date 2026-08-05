---
type: Rust Method
title: expand_jmap_dependency_change
resource: crates/lpe-storage/src/protocols.rs#L535-L608
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-storage/src/protocols/Storage/replay_jmap_object_changes
---

# Signature

`async fn expand_jmap_dependency_change( &self, tenant_id: &Uuid, data_type: &str, object_kind: &str, object_id: Uuid, summary_json: &Value, ) -> Result<Option<Vec<Uuid>>>`

# Calls

- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [replay_jmap_object_changes](../../../../../../functions/crates/lpe-storage/src/protocols/Storage/replay_jmap_object_changes.md)
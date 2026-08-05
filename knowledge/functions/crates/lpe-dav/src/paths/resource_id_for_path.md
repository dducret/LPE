---
type: Rust Function
title: resource_id_for_path
resource: crates/lpe-dav/src/paths.rs#L100-L114
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-dav/src/paths/resource_id_for_contact_path
  - functions/crates/lpe-dav/src/paths/resource_id_for_event_path
  - functions/crates/lpe-dav/src/paths/resource_id_for_task_path
---

# Signature

`pub(crate) fn resource_id_for_path( path: &str, prefix: &str, suffix: &str, ) -> Option<(String, Uuid)>`

# Called by

- [resource_id_for_contact_path](../../../../../functions/crates/lpe-dav/src/paths/resource_id_for_contact_path.md)
- [resource_id_for_event_path](../../../../../functions/crates/lpe-dav/src/paths/resource_id_for_event_path.md)
- [resource_id_for_task_path](../../../../../functions/crates/lpe-dav/src/paths/resource_id_for_task_path.md)
---
type: Rust Function
title: collection_id_from_path
resource: crates/lpe-dav/src/paths.rs#L75-L82
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  called_by:
  - functions/crates/lpe-dav/src/paths/task_collection_id_from_path
  - functions/crates/lpe-dav/src/paths/collection_id_from_contact_path
  - functions/crates/lpe-dav/src/paths/collection_id_from_event_path
---

# Signature

`pub(crate) fn collection_id_from_path(path: &str, prefix: &str) -> Option<String>`

# Calls

- [next](../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)

# Called by

- [task_collection_id_from_path](../../../../../functions/crates/lpe-dav/src/paths/task_collection_id_from_path.md)
- [collection_id_from_contact_path](../../../../../functions/crates/lpe-dav/src/paths/collection_id_from_contact_path.md)
- [collection_id_from_event_path](../../../../../functions/crates/lpe-dav/src/paths/collection_id_from_event_path.md)
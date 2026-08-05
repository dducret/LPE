---
type: Rust Function
title: task_collection_href
resource: crates/lpe-dav/src/paths.rs#L47-L49
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-dav/src/paths/event_collection_href
  - functions/crates/lpe-dav/src/paths/dav_task_collection_id
  called_by:
  - functions/crates/lpe-dav/src/propfind/task_collection_entry
---

# Signature

`pub(crate) fn task_collection_href(collection_id: &str) -> String`

# Calls

- [event_collection_href](../../../../../functions/crates/lpe-dav/src/paths/event_collection_href.md)
- [dav_task_collection_id](../../../../../functions/crates/lpe-dav/src/paths/dav_task_collection_id.md)

# Called by

- [task_collection_entry](../../../../../functions/crates/lpe-dav/src/propfind/task_collection_entry.md)
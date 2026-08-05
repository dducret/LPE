---
type: Rust Function
title: event_collection_href
resource: crates/lpe-dav/src/paths.rs#L32-L34
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-dav/src/paths/task_collection_href
  - functions/crates/lpe-dav/src/propfind/calendar_collection_entry
---

# Signature

`pub(crate) fn event_collection_href(collection_id: &str) -> String`

# Called by

- [task_collection_href](../../../../../functions/crates/lpe-dav/src/paths/task_collection_href.md)
- [calendar_collection_entry](../../../../../functions/crates/lpe-dav/src/propfind/calendar_collection_entry.md)
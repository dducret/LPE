---
type: Rust Function
title: task_report_entry
resource: crates/lpe-dav/src/propfind.rs#L180-L190
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-dav/src/serialize/serialize_vtodo
  - functions/crates/lpe-dav/src/responses/response_entry
  - functions/crates/lpe-dav/src/paths/task_href
---

# Signature

`pub(crate) fn task_report_entry(task: DavTask) -> String`

# Calls

- [serialize_vtodo](../../../../../functions/crates/lpe-dav/src/serialize/serialize_vtodo.md)
- [response_entry](../../../../../functions/crates/lpe-dav/src/responses/response_entry.md)
- [task_href](../../../../../functions/crates/lpe-dav/src/paths/task_href.md)
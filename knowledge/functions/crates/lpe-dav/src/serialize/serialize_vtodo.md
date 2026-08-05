---
type: Rust Function
title: serialize_vtodo
resource: crates/lpe-dav/src/serialize.rs#L56-L83
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-dav/src/serialize/push_line
  - functions/crates/lpe-dav/src/serialize/vtodo_status_from_task_status
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-dav/src/serialize/push_raw_line
  called_by:
  - functions/crates/lpe-dav/src/paths/etag_for_task
  - functions/crates/lpe-dav/src/propfind/task_resource_entry
  - functions/crates/lpe-dav/src/propfind/task_report_entry
  - functions/crates/lpe-dav/src/service/DavService/handle_get
---

# Signature

`pub(crate) fn serialize_vtodo(task: &DavTask) -> String`

# Calls

- [push_line](../../../../../functions/crates/lpe-dav/src/serialize/push_line.md)
- [vtodo_status_from_task_status](../../../../../functions/crates/lpe-dav/src/serialize/vtodo_status_from_task_status.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [push_raw_line](../../../../../functions/crates/lpe-dav/src/serialize/push_raw_line.md)

# Called by

- [etag_for_task](../../../../../functions/crates/lpe-dav/src/paths/etag_for_task.md)
- [task_resource_entry](../../../../../functions/crates/lpe-dav/src/propfind/task_resource_entry.md)
- [task_report_entry](../../../../../functions/crates/lpe-dav/src/propfind/task_report_entry.md)
- [handle_get](../../../../../functions/crates/lpe-dav/src/service/DavService/handle_get.md)
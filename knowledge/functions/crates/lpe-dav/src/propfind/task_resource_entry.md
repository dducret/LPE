---
type: Rust Function
title: task_resource_entry
resource: crates/lpe-dav/src/propfind.rs#L149-L166
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-dav/src/serialize/serialize_vtodo
  - functions/crates/lpe-dav/src/responses/response_entry
  - functions/crates/lpe-dav/src/paths/task_href
  - functions/crates/lpe-dav/src/propfind/collection_props
  - functions/crates/lpe-dav/src/paths/etag
  - functions/crates/lpe-dav/src/propfind/collection_metadata
---

# Signature

`pub(crate) fn task_resource_entry(task: DavTask) -> String`

# Calls

- [serialize_vtodo](../../../../../functions/crates/lpe-dav/src/serialize/serialize_vtodo.md)
- [response_entry](../../../../../functions/crates/lpe-dav/src/responses/response_entry.md)
- [task_href](../../../../../functions/crates/lpe-dav/src/paths/task_href.md)
- [collection_props](../../../../../functions/crates/lpe-dav/src/propfind/collection_props.md)
- [etag](../../../../../functions/crates/lpe-dav/src/paths/etag.md)
- [collection_metadata](../../../../../functions/crates/lpe-dav/src/propfind/collection_metadata.md)
---
type: Rust Function
title: task_collection_id_from_path
resource: crates/lpe-dav/src/paths.rs#L40-L45
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-dav/src/paths/collection_id_from_path
  called_by:
  - functions/crates/lpe-dav/src/service/DavService/handle_propfind
  - functions/crates/lpe-dav/src/service/DavService/handle_report
---

# Signature

`pub(crate) fn task_collection_id_from_path(path: &str) -> Option<String>`

# Calls

- [collection_id_from_path](../../../../../functions/crates/lpe-dav/src/paths/collection_id_from_path.md)

# Called by

- [handle_propfind](../../../../../functions/crates/lpe-dav/src/service/DavService/handle_propfind.md)
- [handle_report](../../../../../functions/crates/lpe-dav/src/service/DavService/handle_report.md)
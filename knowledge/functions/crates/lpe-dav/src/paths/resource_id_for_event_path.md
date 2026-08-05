---
type: Rust Function
title: resource_id_for_event_path
resource: crates/lpe-dav/src/paths.rs#L88-L90
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-dav/src/paths/resource_id_for_path
  called_by:
  - functions/crates/lpe-dav/src/service/DavService/handle_put
  - functions/crates/lpe-dav/src/service/DavService/handle_delete
  - functions/crates/lpe-dav/src/service/DavService/event_for_path
---

# Signature

`pub(crate) fn resource_id_for_event_path(path: &str) -> Option<(String, Uuid)>`

# Calls

- [resource_id_for_path](../../../../../functions/crates/lpe-dav/src/paths/resource_id_for_path.md)

# Called by

- [handle_put](../../../../../functions/crates/lpe-dav/src/service/DavService/handle_put.md)
- [handle_delete](../../../../../functions/crates/lpe-dav/src/service/DavService/handle_delete.md)
- [event_for_path](../../../../../functions/crates/lpe-dav/src/service/DavService/event_for_path.md)
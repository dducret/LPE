---
type: Rust Method
title: event_for_path
resource: crates/lpe-dav/src/service.rs#L436-L450
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-dav/src/paths/resource_id_for_event_path
  called_by:
  - functions/crates/lpe-dav/src/service/DavService/handle_propfind
  - functions/crates/lpe-dav/src/service/DavService/handle_get
  - functions/crates/lpe-dav/src/service/DavService/handle_put
  - functions/crates/lpe-dav/src/service/DavService/handle_delete
---

# Signature

`async fn event_for_path( &self, account_id: Uuid, path: &str, ) -> Result<Option<AccessibleEvent>>`

# Calls

- [resource_id_for_event_path](../../../../../../functions/crates/lpe-dav/src/paths/resource_id_for_event_path.md)

# Called by

- [handle_propfind](../../../../../../functions/crates/lpe-dav/src/service/DavService/handle_propfind.md)
- [handle_get](../../../../../../functions/crates/lpe-dav/src/service/DavService/handle_get.md)
- [handle_put](../../../../../../functions/crates/lpe-dav/src/service/DavService/handle_put.md)
- [handle_delete](../../../../../../functions/crates/lpe-dav/src/service/DavService/handle_delete.md)